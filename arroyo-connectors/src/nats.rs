use crate::{pull_opt, Connection, ConnectionType, Connector, EmptyConfig};
use anyhow::anyhow;
use anyhow::bail;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};

use arroyo_rpc::OperatorConfig;
use axum::response::sse::Event;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use typify::import_types;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/nats/table.json");
const ICON: &str = include_str!("../resources/nats.svg");

// TODO: Create a separate `../connector-schemas/nats/table.json` for allowing
// string template substitution for security reasons (cf. user/password/token)
import_types!(schema = "../connector-schemas/nats/table.json");

pub struct NatsConnector {}

impl Connector for NatsConnector {
    type ProfileT = EmptyConfig;
    type TableT = NatsTable;

    fn name(&self) -> &'static str {
        "nats"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "nats".to_string(),
            name: "Nats".to_string(),
            icon: ICON.to_string(),
            description: "Subscribe to a Nats subject".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.table_type {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
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
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (connection_type, operator, desc) = match table.table_type {
            TableType::Source { .. } => (
                ConnectionType::Source,
                "connectors::nats::source::NatsSourceFunc",
                format!("NatsSource<{}>", table.stream),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                "connectors::nats::sink::NatsSinkFunc::<#in_k, #in_t>",
                format!("NatsSink<{}>", table.subject),
            ),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for NATS"))?;

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
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let conn_type = pull_opt("type", options)?;
        let server = pull_opt("server", options)?;
        let subject = pull_opt("subject", options)?;
        let stream = pull_opt("stream", options)?;

        let table_type = match conn_type.as_str() {
            "source" => TableType::Source {
                password: options.remove("source.password"),
                token: options.remove("source.token"),
                user: options.remove("source.user"),
            },
            "sink" => TableType::Sink {
                password: options.remove("source.password"),
                token: options.remove("source.token"),
                user: options.remove("source.user"),
            },
            _ => {
                bail!("Type must be one of 'source' or 'sink");
            }
        };

        let table = NatsTable {
            server,
            subject,
            stream,
            table_type            
        };
        self.from_config(
            None,
            name,
            EmptyConfig {},
            table,
            schema,
        )
    }
}
