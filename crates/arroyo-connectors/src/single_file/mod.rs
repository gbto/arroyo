use anyhow::{anyhow, bail, Result};
use arroyo_formats::ser::ArrowSerializer;
use typify::import_types;

use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};

use crate::EmptyConfig;

use crate::single_file::sink::SingleFileSink;
use crate::single_file::source::SingleFileSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(schema = "src/single_file/table.json");

mod sink;
mod source;

pub struct SingleFileConnector {}

impl Connector for SingleFileConnector {
    type ProfileT = EmptyConfig;

    type TableT = SingleFileTable;

    fn name(&self) -> &'static str {
        "single_file"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "single_file_source".to_string(),
            name: "Single File Source".to_string(),
            icon: "".to_string(),
            description: "Read a single file".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: true,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.table_type {
            TableType::Source => ConnectionType::Source,
            TableType::Sink => ConnectionType::Sink,
        }
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for Single File Source connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Single File Source connection"))?;
        let connection_type = (&table.table_type).into();

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            connection_type,
            schema,
            &config,
            "Single File Source".to_string(),
        ))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let path = options.pull_str("path")?;
        let Ok(table_type) = options.pull_str("type")?.try_into() else {
            bail!("'type' must be 'source' or 'sink'");
        };

        let wait_for_control = options.pull_opt_bool("wait_for_control")?;

        self.from_config(
            None,
            name,
            EmptyConfig {},
            SingleFileTable {
                path,
                table_type,
                wait_for_control,
            },
            schema,
        )
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        match table.table_type {
            TableType::Source => Ok(ConstructedOperator::from_source(Box::new(
                SingleFileSourceFunc {
                    input_file: table.path,
                    lines_read: 0,
                    format: config
                        .format
                        .expect("Format must be set for Single File Source"),
                    framing: config.framing,
                    bad_data: config.bad_data,
                    wait_for_control: table.wait_for_control.unwrap_or(true),
                },
            ))),
            TableType::Sink => Ok(ConstructedOperator::from_operator(Box::new(
                SingleFileSink {
                    output_path: table.path,
                    file: None,
                    serializer: ArrowSerializer::new(
                        config
                            .format
                            .expect("Format must be set for Single File Sink"),
                    ),
                },
            ))),
        }
    }
}

impl From<&TableType> for ConnectionType {
    fn from(value: &TableType) -> Self {
        match value {
            TableType::Source => ConnectionType::Source,
            TableType::Sink => ConnectionType::Sink,
        }
    }
}
