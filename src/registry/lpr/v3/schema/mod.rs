//! Schema definitions for LPR3 registries

pub mod diagnoser;
pub mod kontakter;
pub mod schema_unified;

pub use diagnoser::lpr3_diagnoser_schema;
pub use kontakter::lpr3_kontakter_schema;
pub use schema_unified as lpr3_unified;