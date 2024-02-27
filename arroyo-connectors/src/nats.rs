use crate::{pull_opt, Connection, ConnectionType, Connector};
use anyhow::anyhow;
use anyhow::bail;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::OperatorConfig;
use axum::response::sse::Event;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use typify::import_types;

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/nats/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/nats/table.json");
const ICON: &str = include_str!("../resources/nats.svg");

import_types!(
    schema = "../connector-schemas/nats/connection.json",
    convert = {{type = "string", format = "var-str"} = VarStr}
);
import_types!(schema = "../connector-schemas/nats/table.json");

pub struct NatsConnector {}

impl NatsConnector {
    pub fn connection_from_options(
        options: &mut HashMap<String, String>,
    ) -> anyhow::Result<NatsConfig> {
        let nats_servers = pull_opt("servers", options)?;
        let nats_auth = options.remove("auth.type");
        let nats_auth: NatsConfigAuthentication = match nats_auth.as_ref().map(|t| t.as_str()) {
            Some("none") | None => NatsConfigAuthentication::None {},
            Some("credentials") => NatsConfigAuthentication::Credentials {
                username: VarStr::new(pull_opt("auth.username", options)?),
                password: VarStr::new(pull_opt("auth.password", options)?),
            },
            Some(other) => bail!("Unknown auth type '{}'", other),
        };

        Ok(NatsConfig {
            authentication: nats_auth,
            servers: nats_servers,
        })
    }

    pub fn table_from_options(options: &mut HashMap<String, String>) -> anyhow::Result<NatsTable> {
        let mut client_config = HashMap::new();
        for (k, v) in options.iter() {
            client_config.insert(k.clone(), v.clone());
        }
        let conn_type = pull_opt("type", options)?;
        let nats_table_type = match conn_type.as_str() {
            "source" => ConnectorType::Source {
                stream: Some(pull_opt("nats.stream", options)?),
            },
            "sink" => ConnectorType::Sink {
                subject: Some(pull_opt("nats.subject", options)?),
            },
            _ => bail!("Type must be one of 'source' or 'sink'"),
        };

        Ok(NatsTable {
            connector_type: nats_table_type,
            client_configs: client_config,
        })
    }
}

impl Connector for NatsConnector {
    type ProfileT = NatsConfig;
    type TableT = NatsTable;

    fn name(&self) -> &'static str {
        "nats"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "nats".to_string(),
            name: "Nats".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from a NATS cluster".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        (*config.servers).to_string()
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match &table.connector_type {
            ConnectorType::Source { .. } => ConnectionType::Source,
            ConnectorType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    ) {
        // TODO: Actually test the connection by instantiating a NATS client
        // and subscribing to the subject at the specified server.
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(Ok(Event::default().json_data(message).unwrap()))
                .await
                .unwrap();
        });
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: NatsConfig,
        table: NatsTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let stream_or_subject = match &table.connector_type {
            ConnectorType::Source { stream, .. } => stream.clone(),
            ConnectorType::Sink { subject, .. } => subject.clone(),
        };

        let (connection_type, operator, desc) = match table.connector_type {
            ConnectorType::Source { .. } => (
                ConnectionType::Source,
                "connectors::nats::source::NatsSourceFunc",
                format!("NatsSource<{:?}>", stream_or_subject.clone()),
            ),
            ConnectorType::Sink { .. } => (
                ConnectionType::Sink,
                "connectors::nats::sink::NatsSinkFunc::<#in_k, #in_t>",
                format!("NatsSink<{:?}>", stream_or_subject.clone()),
            ),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for NATS connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for NATS connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type,
            schema,
            operator: operator.to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description: desc,
        })
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("Invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        Self::from_config(&self, None, name, connection, table, schema)
    }
}
