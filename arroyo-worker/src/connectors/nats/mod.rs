use arroyo_rpc::var_str::VarStr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use typify::import_types;
pub mod sink;
pub mod source;

import_types!(schema = "../connector-schemas/nats/connection.json",
convert = {
    {type = "string", format = "var-str"} = VarStr
});
import_types!(schema = "../connector-schemas/nats/table.json");

fn consumer_configs(connection: &NatsConfig, table: &NatsTable) -> HashMap<String, String> {
    let mut consumer_configs: HashMap<String, String> = HashMap::new();

    match &connection.authentication {
        NatsConfigAuthentication::None {} => {}
        NatsConfigAuthentication::Credentials { username, password } => {
            consumer_configs.insert(
                "nats.username".to_string(),
                username
                    .sub_env_vars()
                    .expect("Missing env-vars for NATS username"),
            );
            consumer_configs.insert(
                "nats.password".to_string(),
                password
                    .sub_env_vars()
                    .expect("Missing env-vars for NATS password"),
            );
            consumer_configs.extend(
                table
                    .client_configs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string())),
            );
        }
    };
    consumer_configs
}
