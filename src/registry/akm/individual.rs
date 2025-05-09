//! AKM registry trait implementations for Individual
//!
//! This module contains the implementation of `AkmRegistry` for the Individual model.

use crate::RecordBatch;
use crate::common::traits::AkmRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use crate::utils::field_extractors::{extract_float64, extract_string};

impl AkmRegistry for Individual {
    fn from_akm_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the serde_arrow-based deserializer for the row
        crate::registry::akm::deserializer::deserialize_row(batch, row)
    }

    fn from_akm_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the serde_arrow-based deserializer for the batch
        crate::registry::akm::deserializer::deserialize_batch(batch)
    }

    fn enhance_with_employment_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Extract employment-related fields
        if let Ok(Some(occupation_code)) = extract_string(batch, row, "DISCO", false) {
            self.occupation_code = Some(occupation_code);
        }

        if let Ok(Some(industry_code)) = extract_string(batch, row, "BRANCHE", false) {
            self.industry_code = Some(industry_code);
        }

        if let Ok(Some(workplace_id)) = extract_string(batch, row, "ARB_STED_ID", false) {
            self.workplace_id = Some(workplace_id);
        }

        if let Ok(Some(hours)) = extract_float64(batch, row, "HELTID", false) {
            self.working_hours = Some(hours);
        }

        Ok(true)
    }
}
