use arroyo_formats::{DataDeserializer, SchemaData};
use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::formats::BadData;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_rpc::OperatorConfig;
use arroyo_state::tables::global_keyed_map::GlobalKeyedState;
use arroyo_types::UserError;
use async_nats;
use bincode::{Decode, Encode};
use crate::{RateLimiter, SourceFinishType};
use crate::engine::Context;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::time::SystemTime;
use tokio::select;
use tracing::debug;
use tracing::info;
use typify::import_types;

import_types!(
    schema = "../connector-schemas/nats/table.json",
    convert = { {type = "string", format = "var-str"} = VarStr }
);

// TODO: Should generic types be more specific here?
#[derive(StreamNode)]
pub struct NatsSourceFunc<K, T>
where
    K: Send + 'static,
    T: SchemaData,
{
    server: String,
    subject: String,
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

    async fn on_start(&mut self, ctx: &mut Context<(), T>) {
        let _s: GlobalKeyedState<(), NatsSourceState, _> =
            ctx.state.get_global_keyed_state('s').await;
        // TODO: Implement state management here
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;
                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    async fn run_int(&mut self, ctx: &mut Context<(), T>) -> Result<SourceFinishType, UserError> {
        let nats_server = self.server.clone();
        let nats_subject = self.subject.clone();

        let client = async_nats::connect(nats_server.clone()).await.unwrap();
        let mut subscriber = client.subscribe(nats_subject.clone()).await.unwrap();

        loop {
            select! {
                message = subscriber.next() => {
                    match message {
                        Some(msg) => {
                            let timestamp = SystemTime::now();
                            let message = self.deserializer.deserialize_single(&msg.payload);

                            debug!("Message format: {:?}", self.deserializer.get_format());
                            debug!("Message payload: {:?}", message.as_ref().unwrap());

                            ctx.collect_source_record(
                                timestamp,
                                message,
                                &self.bad_data,
                                &mut self.rate_limiter,
                            ).await?;
                        },
                        None => {
                            break
                            info!("Finished reading message from {}", &nats_subject);
                        },
                    }
                }
                // TODO: Implement checkpointing here
                // control_message = ctx.control_rx.recv() => {
                //     match control_message {
                //         Some(msg) => {
                //         },
                //         None => {
                //         }
                //     }
                // }
            }
        }
        Ok(SourceFinishType::Graceful)
    }
}
