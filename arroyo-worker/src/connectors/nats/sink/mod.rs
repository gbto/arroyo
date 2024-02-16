use anyhow::Result;
use arroyo_formats::DataSerializer;
use arroyo_formats::SchemaData;
use arroyo_macro::process_fn;
use arroyo_rpc::{ControlResp, OperatorConfig};
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_types::*;
use crate::engine::{Context, StreamNode};
use serde::Serialize;
use std::marker::PhantomData;
use super::NatsTable;
use tracing::warn;
use arroyo_rpc::ControlMessage;
use tracing::info;

#[derive(StreamNode)]
pub struct NatsSinkFunc<K: Key + Serialize, T: SchemaData>  {
    publisher: Option<async_nats::Client>,
    server: String,
    subject: String,
    user: Option<String>,
    password: Option<String>,
    serializer: DataSerializer<T>,
    _t: PhantomData<K>,
}

impl <K: Key + Serialize, T: SchemaData> NatsSinkFunc<K, T> {
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for NatSinkFunc");
        let table: NatsTable =
            serde_json::from_value(config.table).expect("Invalid table config for NatsSinkFunc");
        let format = config
            .format
            .expect("NATS source must have a format configured");
        Self {
            publisher: None,
            server: table.server,
            subject: table.subject,
            user: table.table_type.get_credentials("user"),
            password: table.table_type.get_credentials("password"),
            serializer: DataSerializer::new(format),
            _t: PhantomData,
        }
    }   
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key + Serialize, T: SchemaData> NatsSinkFunc<K, T> {

    fn name(&self) -> String {
        format!("nats-publisher-{}", self.subject)
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("s", "NATS sink state")]
    }

    async fn get_nats_client(&mut self) -> Result<async_nats::Client> {
        info!("Creating NATS publisher for {:?}", self.subject);
        let nats_client: async_nats::Client = async_nats::ConnectOptions::new()
            .user_and_password(self.user.clone().unwrap(), self.password.clone().unwrap())
            .connect(self.server.clone())
            .await
            .unwrap();
        Ok(nats_client)
    }

    async fn on_start(&mut self, _ctx: &mut Context<(), ()>) {
        // TODO: Get the NATS state sequence_number, i.e. what's the last
        // message that was succesfully published to the NATS server
        match self.get_nats_client().await {
            Ok(client) => {
                self.publisher = Some(client);
            }
            Err(e) => {
                panic!("Failed to construct NATS publisher: {:?}", e);
            }
        }
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<(), ()>) {
        let nats_subject = async_nats::Subject::from(self.subject.clone());
        let nats_message = serde_json::to_string(&record.value).unwrap();

        match self.publisher.as_mut().unwrap().publish(nats_subject, nats_message.into()).await {
            Ok(_) => {}
            Err(e) => {
                ctx.control_tx
                    .send(ControlResp::Error {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index,
                        message: e.to_string(),
                        details: e.to_string(),
                    })
                    .await
                    .unwrap();
                panic!("Panicked while processing element: {}", e.to_string());
            }
        }
    }

    async fn handle_checkpoint(
        &mut self,
        _: &arroyo_types::CheckpointBarrier,
        _: &mut crate::engine::Context<(), ()>,
    ) {
        self.publisher.as_mut().unwrap().flush().await.unwrap();
    }

    async fn on_close(
        &mut self,
        ctx: &mut crate::engine::Context<(), ()>,
        _final_message: &Option<Message<(), ()>>,
    ) {
        if let Some(ControlMessage::Commit { epoch, commit_data }) = ctx.control_rx.recv().await {
            self.handle_commit(epoch, commit_data, ctx).await;
        } else {
            warn!("No commit message received, not committing")
        }
    }
}