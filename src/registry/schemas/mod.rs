//! Schema definitions for Danish registry data sources
//!
//! This module contains Arrow schema definitions for all supported registries.

pub mod akm;
pub mod bef;
pub mod dod;
pub mod dodsaarsag;
pub mod idan;
pub mod ind;
pub mod lpr_adm;
pub mod lpr_diag;
pub mod lpr_bes;
pub mod lpr3_kontakter;
pub mod lpr3_diagnoser;
pub mod mfr;
pub mod uddf;
pub mod vnds;

// Re-export schema functions for easier access
pub use akm::akm_schema;
pub use bef::bef_schema;
pub use dod::dod_schema;
pub use dodsaarsag::dodsaarsag_schema;
pub use idan::idan_schema;
pub use ind::ind_schema;
pub use lpr_adm::lpr_adm_schema;
pub use lpr_diag::lpr_diag_schema;
pub use lpr_bes::lpr_bes_schema;
pub use lpr3_kontakter::lpr3_kontakter_schema;
pub use lpr3_diagnoser::lpr3_diagnoser_schema;
pub use mfr::mfr_schema;
pub use uddf::uddf_schema;
pub use vnds::vnds_schema;