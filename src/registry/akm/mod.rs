//! AKM registry using the macro-based approach
//!
//! The AKM (Arbejdsklassifikationsmodulet) registry contains employment information.

use chrono::NaiveDate;
use crate::{RegistryTrait, error, models, registry, schema};

/// Labour register with employment information
#[derive(RegistryTrait, Debug)]
#[registry(name = "AKM", description = "Labour register")]
pub struct AkmRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Socioeconomic status code
    #[field(name = "SOCIO13")]
    pub socioeconomic_status: Option<String>,
}

/// Helper function to create a new AKM deserializer
pub fn create_deserializer() -> AkmRegistryDeserializer {
    AkmRegistryDeserializer::new()
}