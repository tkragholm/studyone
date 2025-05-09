//! VNDS registry trait implementations for Individual
//! 
//! This module contains the implementation of VndsRegistry for the Individual model.

use crate::RecordBatch;
use crate::common::traits::VndsRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use crate::registry::deserializer;
use crate::utils::field_extractors::extract_date32;

impl VndsRegistry for Individual {
    fn from_vnds_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        deserializer::deserialize_minimal_row(batch, row)
    }

    fn from_vnds_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        deserializer::deserialize_minimal(batch)
    }

    fn enhance_with_migration_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Extract migration-related fields
        if let Ok(Some(emigration_date)) = extract_date32(batch, row, "UDRDTO", false) {
            self.emigration_date = Some(emigration_date);
        }

        if let Ok(Some(immigration_date)) = extract_date32(batch, row, "INDRDTO", false) {
            self.immigration_date = Some(immigration_date);
        }

        Ok(true)
    }
}