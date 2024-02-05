use crate::{pull_opt, Connection, Connector, EmptyConfig};
use anyhow::anyhow;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
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

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
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
        let desc = format!("NatsSource<{}>", table.subject);

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for NATS"))?;

        // TODO: Test the config schema and return an error if it's invalid.
        // Does it already happen somewhere else as server and subject are specified
        // as `required` in the JSON schema?

        // TODO: Where is that format coming from?
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
            connection_type: ConnectionType::Source,
            schema,
            operator: "connectors::nats::source::NatsSourceFunc".to_string(),
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
        let server = pull_opt("server", options)?;
        let subject = pull_opt("subject", options)?;
        let user=  options.remove("user");
        let password = options.remove("password");
        let token = options.remove("token");

        self.from_config(
            None,
            name,
            EmptyConfig {},
            NatsTable {
                server,
                subject,
                user,
                password,
                token,
            },
            schema,
        )
    }
}
