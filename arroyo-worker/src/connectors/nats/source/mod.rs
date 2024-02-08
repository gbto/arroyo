use crate::engine::Context;
use crate::{RateLimiter, SourceFinishType};
use arroyo_formats::{DataDeserializer, SchemaData};
use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::formats::BadData;
use arroyo_rpc::grpc::{StopMode, TableDescriptor};
use arroyo_rpc::ControlMessage;
use arroyo_rpc::ControlResp;
use arroyo_rpc::OperatorConfig;
use arroyo_types::UserError;
use bincode::{Decode, Encode};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::time::SystemTime;
use tokio::select;
use tracing::debug;
use tracing::info;
use typify::import_types;
use async_nats::jetstream::consumer::push::Config;


import_types!(schema = "../connector-schemas/nats/table.json");

// TODO: Should generic types be more specific here?
#[derive(StreamNode)]
pub struct NatsSourceFunc<K, T>
where
    K: Send + 'static,
    T: SchemaData,
{
    server: String,
    subject: String,
    user: Option<String>,
    password: Option<String>,
    deserializer: DataDeserializer<T>,
    bad_data: Option<BadData>,
    rate_limiter: RateLimiter,
    _t: PhantomData<(K, T)>,
}

// TODO: How to handle the state of a NATS source? last_message like `polling_http`?
#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
enum NatsSourceState {
    Finished,
    RecordsRead(usize),
}

pub fn tables() -> Vec<TableDescriptor> {
    vec![arroyo_state::global_table("f", "NATS source state")]
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> NatsSourceFunc<K, T>
where
    K: Send + 'static,
    T: SchemaData,
{
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for NatSourceFunc");
        let table: NatsTable =
            serde_json::from_value(config.table).expect("Invalid table config for NatsSourceFunc");
        let format = config
            .format
            .expect("NATS source must have a format configured");
        let framing = config.framing;

        Self {
            server: table.server,
            subject: table.subject,
            user: table.user,
            password: table.password,
            deserializer: DataDeserializer::new(format, framing),
            bad_data: config.bad_data,
            rate_limiter: RateLimiter::new(),
            _t: PhantomData,
        }
    }
    
    fn name(&self) -> String {
        "NatsSource".to_string()
    }

    pub fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![arroyo_state::global_table("s", "NATS source state")]
    }

    async fn get_nats_consumer(&mut self, 
            stream_name: &str,
            consumer_name: &str,
        ) -> async_nats::jetstream::consumer::Consumer<Config> {
        let client = async_nats::ConnectOptions::new()
            .user_and_password(self.user.clone().unwrap(), self.password.clone().unwrap())
            .connect(self.server.clone())
            .await
            .unwrap();
        let jetstream = async_nats::jetstream::new(client);
        let stream = jetstream.get_stream(stream_name).await.unwrap();
        let consumer = stream.get_consumer(consumer_name).await.unwrap();
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

        // TODO: Add stream and consumer name as configuration options
        let consumer = self.get_nats_consumer(
            "balances-demo",
            "balances-demo-push",
        ).await;
        let mut messages = consumer.messages().await.unwrap();

        loop {
            select! {
                message = messages.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            let timestamp = SystemTime::now();
                            let payload = msg.payload.as_ref();
                            let message = self.deserializer.deserialize_single(&payload);

                            debug!("Message format: {:?}", self.deserializer.get_format());
                            debug!("Message payload: {:?}", message.as_ref().unwrap());

                            ctx.collect_source_record(
                                timestamp,
                                message,
                                &self.bad_data,
                                &mut self.rate_limiter,
                            ).await?;
                        },
                        Some(Err(msg)) => {
                            return Err(UserError::new("NATS message error", msg.to_string()));
                        },
                        None => {
                            break
                            info!("Finished reading message from {}", &self.subject);
                        },
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("Starting checkpointing {}", ctx.task_info.task_index);
                            // let mut s = ctx.state.get_global_keyed_state('k').await;

                            // TODO: Implement checkpointing here. In Kafka, checkpointing is handled
                            // via messages offsets in their respective partitions to guarantee exactly
                            // once semantics. In NATS, messages are published to subjects (topics),
                            // and subscribers can receive messages from those subjects. NATS doesn't
                            // maintain offsets or partitions for subscribers because it focuses on
                            // simple, lightweight, and real-time message distribution.

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
