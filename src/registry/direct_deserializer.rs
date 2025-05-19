//! Direct Individual deserializer
//!
//! This module provides a deserializer that directly maps registry data to Individual models
//! without requiring intermediate registry-specific structs.

use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::models::core::Individual;
use crate::registry::trait_deserializer::{RegistryDeserializer, RegistryFieldExtractor};

use crate::schema::{RegistrySchema, create_registry_schema};

// Import field mapping modules from registry-specific modules
use crate::registry::akm::field_mapping as akm_mapping;
use crate::registry::bef::field_mapping as bef_mapping;
use crate::registry::death::dod::field_mapping as dod_mapping;
use crate::registry::death::dodsaarsag::field_mapping as dodsaarsag_mapping;
use crate::registry::ind::field_mapping as ind_mapping;
use crate::registry::lpr::v2::adm::field_mapping as lpr_adm_mapping;
use crate::registry::lpr::v2::bes::field_mapping as lpr_bes_mapping;
use crate::registry::lpr::v2::diag::field_mapping as lpr_diag_mapping;
use crate::registry::lpr::v3::diagnoser::field_mapping as lpr3_diagnoser_mapping;
use crate::registry::lpr::v3::kontakter::field_mapping as lpr3_kontakter_mapping;
use crate::registry::mfr::field_mapping as mfr_mapping;
use crate::registry::uddf::field_mapping as uddf_mapping;
use crate::registry::vnds::field_mapping as vnds_mapping;

/// Individual deserializer for a specific registry without intermediate struct
#[derive(Debug)]
pub struct DirectIndividualDeserializer {
    inner: Arc<dyn RegistryDeserializer>,
}

impl DirectIndividualDeserializer {
    /// Create a new deserializer for the specified registry type
    ///
    /// # Arguments
    ///
    /// * `registry_name` - The registry name (e.g., "VNDS", "BEF", "AKM", etc.)
    ///
    /// # Returns
    ///
    /// A new instance of the deserializer
    #[must_use] pub fn new(registry_name: &str) -> Self {
        // Create a registry-specific schema based on the registry name
        let schema = match registry_name {
            "VNDS" => Self::create_vnds_schema(),
            "BEF" => Self::create_bef_schema(),
            "AKM" => Self::create_akm_schema(),
            "MFR" => Self::create_mfr_schema(),
            "DOD" => Self::create_dod_schema(),
            "DODSAARSAG" => Self::create_dodsaarsag_schema(),
            "IND" => Self::create_ind_schema(),
            "UDDF" => Self::create_uddf_schema(),
            "LPR_ADM" => Self::create_lpr_adm_schema(),
            "LPR_BES" => Self::create_lpr_bes_schema(),
            "LPR_DIAG" => Self::create_lpr_diag_schema(),
            "LPR3_KONTAKTER" => Self::create_lpr3_kontakter_schema(),
            "LPR3_DIAGNOSER" => Self::create_lpr3_diagnoser_schema(),
            _ => Self::create_default_schema(registry_name),
        };

        // Get PNR column name for this registry type
        let pnr_column = match registry_name {
            "VNDS" => Some("PNR"),
            "BEF" => Some("PNR"),
            "AKM" => Some("PNR"),
            "MFR" => Some("CPR_BARN"),
            "DOD" => Some("PNR"),
            "DODSAARSAG" => Some("PNR"),
            "IND" => Some("PNR"),
            "UDDF" => Some("PNR"),
            "LPR_ADM" => Some("PNR"),
            "LPR3_KONTAKTER" => Some("CPR"),
            "LPR3_DIAGNOSER" => None, // This registry uses contact_id for join
            _ => Some("pnr"),
        };

        // Create a deserializer implementation with the schema
        let inner = Arc::new(
            crate::registry::trait_deserializer_impl::RegistryDeserializerImpl::new(
                registry_name,
                format!("{registry_name} registry"),
                schema,
                pnr_column,
            ),
        );

        Self { inner }
    }

    /// Create VNDS migration registry schema
    fn create_vnds_schema() -> RegistrySchema {
        // Get field mappings from the VNDS registry module
        let field_mappings = vnds_mapping::create_field_mappings();
        create_registry_schema("VNDS", "VNDS Migration registry", field_mappings)
    }

    /// Create BEF population registry schema
    fn create_bef_schema() -> RegistrySchema {
        // Get field mappings from the BEF registry module
        let field_mappings = bef_mapping::create_field_mappings();
        create_registry_schema("BEF", "BEF Population registry", field_mappings)
    }

    /// Create AKM labour registry schema
    fn create_akm_schema() -> RegistrySchema {
        // Get field mappings from the AKM registry module
        let field_mappings = akm_mapping::create_field_mappings();
        create_registry_schema("AKM", "AKM Labour registry", field_mappings)
    }

    /// Create MFR medical birth registry schema
    fn create_mfr_schema() -> RegistrySchema {
        // Get field mappings from the MFR registry module
        let field_mappings = mfr_mapping::create_field_mappings();
        create_registry_schema("MFR", "MFR Medical Birth registry", field_mappings)
    }

    /// Create DOD death registry schema
    fn create_dod_schema() -> RegistrySchema {
        // Get field mappings from the DOD registry module
        let field_mappings = dod_mapping::create_field_mappings();
        create_registry_schema("DOD", "DOD Death registry", field_mappings)
    }

    /// Create DODSAARSAG cause of death registry schema
    fn create_dodsaarsag_schema() -> RegistrySchema {
        // Get field mappings from the DODSAARSAG registry module
        let field_mappings = dodsaarsag_mapping::create_field_mappings();
        create_registry_schema("DODSAARSAG", "DODSAARSAG Cause of Death registry", field_mappings)
    }

    /// Create IND income registry schema
    fn create_ind_schema() -> RegistrySchema {
        // Get field mappings from the IND registry module
        let field_mappings = ind_mapping::create_field_mappings();
        create_registry_schema("IND", "IND Income registry", field_mappings)
    }

    /// Create UDDF education registry schema
    fn create_uddf_schema() -> RegistrySchema {
        // Get field mappings from the UDDF registry module
        let field_mappings = uddf_mapping::create_field_mappings();
        create_registry_schema("UDDF", "UDDF Education registry", field_mappings)
    }

    /// Create `LPR_ADM` admission registry schema
    fn create_lpr_adm_schema() -> RegistrySchema {
        // Get field mappings from the LPR ADM registry module
        let field_mappings = lpr_adm_mapping::create_field_mappings();
        create_registry_schema("LPR_ADM", "LPR Admissions registry", field_mappings)
    }

    /// Create `LPR_BES` outpatient visits registry schema
    fn create_lpr_bes_schema() -> RegistrySchema {
        // Get field mappings from the LPR BES registry module
        let field_mappings = lpr_bes_mapping::create_field_mappings();
        create_registry_schema("LPR_BES", "LPR Outpatient Visits registry", field_mappings)
    }

    /// Create `LPR_DIAG` diagnosis registry schema
    fn create_lpr_diag_schema() -> RegistrySchema {
        // Get field mappings from the LPR DIAG registry module
        let field_mappings = lpr_diag_mapping::create_field_mappings();
        create_registry_schema("LPR_DIAG", "LPR Diagnoses registry", field_mappings)
    }

    /// Create `LPR3_KONTAKTER` contacts registry schema
    fn create_lpr3_kontakter_schema() -> RegistrySchema {
        // Get field mappings from the LPR3 KONTAKTER registry module
        let field_mappings = lpr3_kontakter_mapping::create_field_mappings();
        create_registry_schema("LPR3_KONTAKTER", "LPR v3 Contacts registry", field_mappings)
    }

    /// Create `LPR3_DIAGNOSER` diagnoses registry schema
    fn create_lpr3_diagnoser_schema() -> RegistrySchema {
        // Get field mappings from the LPR3 DIAGNOSER registry module
        let field_mappings = lpr3_diagnoser_mapping::create_field_mappings();
        create_registry_schema("LPR3_DIAGNOSER", "LPR v3 Diagnoses registry", field_mappings)
    }

    /// Create default schema for any registry type
    fn create_default_schema(registry_name: &str) -> RegistrySchema {
        // Get default field mappings from the common module
        let field_mappings =
            crate::registry::common::field_mapping::create_default_field_mappings();

        create_registry_schema(
            registry_name,
            format!("{registry_name} registry"),
            field_mappings,
        )
    }

    /// Deserialize a record batch directly into a vector of Individual models
    ///
    /// # Arguments
    ///
    /// * `batch` - The Arrow record batch to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing a Vec of deserialized Individuals
    pub fn deserialize_batch(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        self.inner.deserialize_batch(batch)
    }

    /// Deserialize a single row from a record batch
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch
    /// * `row` - The row index to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing an Option with the deserialized Individual
    pub fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
        self.inner.deserialize_row(batch, row)
    }

    /// Get field extractors used by this deserializer
    #[must_use] pub fn field_extractors(&self) -> &[Box<dyn RegistryFieldExtractor>] {
        self.inner.field_extractors()
    }

    /// Get field name mapping
    #[must_use] pub fn field_mapping(&self) -> HashMap<String, String> {
        self.inner.field_mapping()
    }
}