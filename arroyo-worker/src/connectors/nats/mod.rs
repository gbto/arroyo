use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod source;

import_types!(schema = "../connector-schemas/nats/table.json");
