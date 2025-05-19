//! Direct Registry Loader
//!
//! This module provides a wrapper around DirectIndividualDeserializer that
//! implements the RegisterLoader trait, enabling direct deserialization without
//! intermediate registry-specific structs.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::registry::RegisterLoader;
use crate::registry::direct_deserializer::DirectIndividualDeserializer;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

/// Registry loader that uses direct deserialization without intermediate structs
#[derive(Debug)]
pub struct DirectRegistryLoader {
    /// The registry name
    registry_name: &'static str,
    /// The inner deserializer
    deserializer: DirectIndividualDeserializer,
    /// PNR column name (if any)
    pnr_column: Option<&'static str>,
}

impl DirectRegistryLoader {
    /// Create a new direct registry loader
    ///
    /// # Arguments
    ///
    /// * `registry_name` - The name of the registry
    ///
    /// # Returns
    ///
    /// A new DirectRegistryLoader
    #[must_use]
    pub fn new(registry_name: &'static str) -> Self {
        // Create a new DirectIndividualDeserializer
        let deserializer = DirectIndividualDeserializer::new(registry_name);

        // Determine PNR column based on registry name
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
            "LPR_BES" => Some("PNR"),
            "LPR_DIAG" => Some("PNR"),
            "LPR3_KONTAKTER" => Some("CPR"),
            "LPR3_DIAGNOSER" => None, // This registry uses contact_id for join
            _ => Some("PNR"),
        };

        Self {
            registry_name,
            deserializer,
            pnr_column,
        }
    }

    /// Deserialize a batch of records
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing a Vec of deserialized Individual models
    pub fn deserialize_batch(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        self.deserializer.deserialize_batch(batch)
    }
}

impl RegisterLoader for DirectRegistryLoader {
    fn get_register_name(&self) -> &'static str {
        self.registry_name
    }

    fn get_schema(&self) -> SchemaRef {
        // Convert the field extractors to an Arrow schema
        let _field_mapping = self.deserializer.field_mapping();
        let field_extractors = self.deserializer.field_extractors();

        // Build the schema from the field definitions
        let fields = field_extractors
            .iter()
            .map(|extractor| {
                let field_def = extractor.get_field_definition();
                let name = field_def.name.clone();
                let data_type = field_def.field_type.to_arrow_type(true);
                let is_nullable = field_def.nullable;
                Arc::new(arrow::datatypes::Field::new(name, data_type, is_nullable))
            })
            .collect::<Vec<_>>();

        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    fn get_pnr_column_name(&self) -> Option<&'static str> {
        self.pnr_column
    }

    fn get_join_column_name(&self) -> Option<&'static str> {
        // LPR3_DIAGNOSER uses contact_id for joins
        if self.registry_name == "LPR3_DIAGNOSER" {
            Some("DW_EK_KONTAKT")
        } else {
            None
        }
    }
}
