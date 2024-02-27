use crate::engine::Context;
use crate::{RateLimiter, SourceFinishType};
use arroyo_formats::{DataDeserializer, SchemaData};
use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::formats::BadData;
use arroyo_rpc::grpc::{StopMode, TableDescriptor};
use arroyo_rpc::ControlMessage;
use arroyo_rpc::ControlResp;
use arroyo_rpc::OperatorConfig;
use arroyo_state::tables::global_keyed_map::GlobalKeyedState;
use arroyo_types::Data;
use arroyo_types::UserError;
use async_nats::jetstream::consumer;
use async_nats::ServerAddr;
use bincode::{Decode, Encode};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};
use tokio::select;
use tracing::debug;
use tracing::info;

use super::ConnectorType;
use super::consumer_configs;
use super::NatsConfig;
use super::NatsTable;

#[derive(StreamNode)]
pub struct NatsSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: SchemaData + Data,
{
    stream: String,
    servers: String,
    deserializer: DataDeserializer<T>,
    bad_data: Option<BadData>,
    rate_limiter: RateLimiter,
    consumer_config: HashMap<String, String>,
    messages_per_second: NonZeroU32,
    _t: PhantomData<(K, T)>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct NatsSourceState {
    stream_name: String,
    stream_sequence_number: u64,
}

pub fn tables() -> Vec<TableDescriptor> {
    vec![arroyo_state::global_table("k", "NATS source state")]
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> NatsSourceFunc<K, T>
where
    K: DeserializeOwned + Data + Send,
    T: SchemaData + Data,
{
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for NatSourceFunc");
        let connection: NatsConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for NatsSourceFunc");
        let table: NatsTable =
            serde_json::from_value(config.table).expect("Invalid table config for NatsSourceFunc");
        let format = config
            .format
            .expect("NATS source must have a format configured");
        let framing = config
            .framing;
        let stream = match &table.connector_type {
            ConnectorType::Source { ref stream, .. } => stream,
            _ => panic!("NATS source must have a stream configured"),
        };

        let consumer_default_config = consumer_configs(&connection, &table);

        Self {
            stream: stream.clone().unwrap(),
            servers: connection.servers,
            deserializer: DataDeserializer::new(format, framing),
            bad_data: config.bad_data,
            rate_limiter: RateLimiter::new(),
            consumer_config: consumer_default_config,
            messages_per_second: NonZeroU32::new(
                config
                    .rate_limit
                    .map(|l| l.messages_per_second)
                    .unwrap_or(u32::MAX),
            )
            .unwrap(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "NatsSource".to_string()
    }

    pub fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![arroyo_state::global_table("k", "NATS source state")]
    }

    async fn get_nats_stream(
        &mut self,
        stream_name: String,
    ) -> async_nats::jetstream::stream::Stream {
        let servers_borrowed = &self.servers;
        let servers_vec: Vec<ServerAddr> = servers_borrowed
            .split(',')
            .map(|s| s.parse().unwrap())
            .collect();
        let client = async_nats::ConnectOptions::new()
            .user_and_password("user1".to_string(), "user1".to_string())
            .connect(servers_vec)
            .await
            .unwrap();

        let jetstream = async_nats::jetstream::new(client);
        let mut stream = jetstream.get_stream(&stream_name).await.unwrap();

        let stream_info = stream.info().await.unwrap();
        info!("<---------------------------------------------->");
        info!("Stream - timestamp of creation: {}", &stream_info.created);
        info!(
            "Stream - lowest sequence number still present: {}",
            &stream_info.state.first_sequence
        );
        info!(
            "Stream - last sequence number assigned to a message: {}",
            &stream_info.state.last_sequence
        );
        info!(
            "Stream - time that the last message was received: {}",
            &stream_info.state.last_timestamp
        );
        info!(
            "Stream - number of messages contained: {}",
            &stream_info.state.messages
        );
        info!(
            "Stream - number of bytes contained: {}",
            &stream_info.state.bytes
        );
        info!(
            "Stream - number of consumers: {}",
            &stream_info.state.consumer_count
        );
        stream
    }

    async fn get_nats_consumer(
        &mut self,
        stream: &async_nats::jetstream::stream::Stream,
        ctx: &mut Context<(), T>,
    ) -> consumer::Consumer<consumer::pull::Config> {
        // Get the last valid state from the checkpoints
        let mut s: GlobalKeyedState<String, NatsSourceState, _> =
            ctx.state.get_global_keyed_state('k').await;

        let state: Vec<&NatsSourceState> = s.get_all();
        let sequence = if !state.is_empty() {
            info!("Found state for NATS source");
            let max_sequence_number = state
                .iter()
                .max_by_key(|state| state.stream_sequence_number)
                .map(|state| state.stream_sequence_number)
                .unwrap();
            info!("Starting from sequence number: {}", max_sequence_number + 1);
            max_sequence_number
        } else {
            info!("No state found for NATS source. All stream messages will be processed.");
            info!("Starting from sequence number: 1");
            1
        };
        // Configure the delivery policy that will define where the consumer will start
        let deliver_policy = {
            if sequence == 0 {
                consumer::DeliverPolicy::All
            } else {
                consumer::DeliverPolicy::ByStartSequence {
                    start_sequence: sequence + 1,
                }
            }
        };
        // Define the consumer configuration
        // TODO: Replace by client_configs object built in module
        let consumer_config = consumer::pull::Config {
            name: Some(self.consumer_config.get("nats.username").unwrap().clone()),
            ack_policy: consumer::AckPolicy::Explicit,
            replay_policy: consumer::ReplayPolicy::Instant,
            inactive_threshold: Duration::from_secs(60),
            ack_wait: Duration::from_secs(60),
            num_replicas: 1,
            deliver_policy,
            ..Default::default()
        };
        match stream
            .delete_consumer(&self.consumer_config.get("nats.username").unwrap().clone())
            .await
        {
            Ok(_) => {
                info!("Existing consumer deleted. Recreating consumer with new `start_sequence`.")
            }
            Err(_) => {
                info!("No existing consumer found, proceeding with the creation of a new one.")
            }
        }
        let mut consumer = stream
            .create_consumer(consumer_config.clone())
            .await
            .unwrap();

        let consumer_info = consumer.info().await.unwrap();
        info!(
            "Consumer - timestamp of creation: {}",
            &consumer_info.created
        );
        info!(
            "Consumer - last stream sequence of aknowledged messagee: {}",
            &consumer_info.ack_floor.stream_sequence
        );
        info!(
            "Consumer - last consumer sequence of aknowledged message: {}",
            &consumer_info.ack_floor.consumer_sequence
        );
        info!(
            "Consumer delivered messages: {}",
            &consumer_info.num_ack_pending
        );
        info!(
            "Consumer pending ack messages: {}",
            &consumer_info.num_pending
        );
        info!(
            "Consumer waiting delivery messages: {}",
            &consumer_info.num_waiting
        );
        info!("<--------------------------------------------->");
        consumer
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.control_tx
                    .send(ControlResp::Error {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index,
                        message: e.name.clone(),
                        details: e.details.clone(),
                    })
                    .await
                    .unwrap();
                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    async fn run_int(&mut self, ctx: &mut Context<(), T>) -> Result<SourceFinishType, UserError> {
        // let config = &self.consumer_config;
        let stream = self.get_nats_stream(self.stream.clone()).await;
        let consumer = self.get_nats_consumer(&stream, ctx).await;

        let mut sequence: HashMap<String, NatsSourceState> = HashMap::new();
        let mut messages = consumer.messages().await.unwrap();

        loop {
            select! {
                message = messages.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            // TODO: Should another timestamp be used here?
                            let timestamp = SystemTime::now();
                            let payload = msg.payload.as_ref();
                            let message = self.deserializer.deserialize_single(&payload);

                            let message_info = msg.info().unwrap();

                            // info!("---------------------------------------------->");
                            // debug!("Message format: {:?}", self.deserializer.get_format());
                            // debug!("Message payload: {:?}", message.as_ref().unwrap());
                            // info!(
                            //     "Delivered stream sequence: {}",
                            //     message_info.stream_sequence
                            // );
                            // info!(
                            //     "Delivered consumer sequence: {}",
                            //     message_info.consumer_sequence
                            // );
                            // info!(
                            //     "Delivered message stream: {}",
                            //     message_info.stream
                            // );
                            // info!(
                            //     "Delivered message consumer: {}",
                            //     message_info.consumer
                            // );
                            // info!(
                            //     "Delivered message published: {}",
                            //     message_info.published
                            // );
                            // info!(
                            //     "Delivered message pending: {}",
                            //     message_info.pending
                            // );
                            // info!(
                            //     "Delivered message delivered: {}",
                            //     message_info.delivered
                            // );

                            ctx.collect_source_record(
                                timestamp,
                                message,
                                &self.bad_data,
                                &mut self.rate_limiter,
                            ).await?;

                            // Inserting the collected sequence number into the a representation
                            // of the state that is yet to be written to the checkpoint
                            let stream_name = message_info.consumer.to_string();
                            let stream_sequence_number = message_info.stream_sequence.clone();
                            sequence.insert(stream_name.clone(), NatsSourceState { stream_name, stream_sequence_number});

                            // TODO: Has ACK to happens here at every message? Maybe it can be
                            // done by ack only the last message before checkpointing
                            msg.ack().await.unwrap();
                        },
                        Some(Err(msg)) => {
                            return Err(UserError::new("NATS message error", msg.to_string()));
                        },
                        None => {
                            break
                            info!("Finished reading message from {}", self.stream);
                        },
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("Starting checkpointing {}", ctx.task_info.task_index);
                            let mut s: GlobalKeyedState<String, NatsSourceState, _> = ctx.state.get_global_keyed_state('k').await;

                            // TODO: Can this be parallelized?
                            for (stream_name, seq_state) in &sequence {
                                s.insert(stream_name.to_string(), seq_state.clone()).await;
                            }

                            let max_sequence_number = sequence
                                .iter()
                                .map(|(_, state)| state.stream_sequence_number)
                                .max()
                                .unwrap();

                            debug!("Checkpointed sequence number: {}", max_sequence_number);

                            if self.checkpoint(c, ctx).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        }
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping NATS source: {:?}", mode);
                            match mode {
                                StopMode::Graceful => {
                                    return Ok(SourceFinishType::Graceful);
                                }
                                StopMode::Immediate => {
                                    return Ok(SourceFinishType::Immediate);
                                }
                            }
                        }
                        Some(ControlMessage::Commit { .. }) => {
                            unreachable!("Sources shouldn't receive commit messages");
                        }
                        Some(ControlMessage::LoadCompacted {compacted}) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(ControlMessage::NoOp) => {}
                        None => {
                        }
                    }
                }
            }
        }
        Ok(SourceFinishType::Graceful)
    }
}
