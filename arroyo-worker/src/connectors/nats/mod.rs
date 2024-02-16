use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod sink;
pub mod source;

import_types!(schema = "../connector-schemas/nats/table.json");

impl TableType {
    fn get_credentials(&self, attribute: &str) -> Option<String> {
        match self {
            TableType::Source { user, password, .. } => {
                if attribute == "user" {
                    user.clone()
                } else if attribute == "password" {
                    password.clone()
                } else {
                    None
                }
            }
            TableType::Sink { user, password, .. } => {
                if attribute == "user" {
                    user.clone()
                } else if attribute == "password" {
                    password.clone()
                } else {
                    None
                }
            }
        }
    }
}
