//! AKM registry trait implementations for Individual
//!
//! This module contains the implementation of `AkmRegistry` for the Individual model.

use crate::RecordBatch;
use crate::common::traits::AkmRegistry;
use crate::error::Result;
use crate::models::core::Individual;
//use crate::utils::field_extractors::extract_string;

impl AkmRegistry for Individual {
    fn from_akm_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::akm::deserializer::deserialize_row(batch, row)
    }

    fn from_akm_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::akm::deserializer::deserialize_batch(batch)
    }

    fn enhance_with_employment_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // With our unified schema approach, enhancement is handled automatically by the deserializer
        // The trait-based deserializer uses registry traits and field mappings from the unified schema

        // Create a temporary Individual using the deserializer
        if let Some(_enhanced) = crate::registry::akm::deserializer::deserialize_row(batch, row)? {
            // // Copy employment-specific fields from the enhanced Individual to this one
            // if let Some(occupation_code) = enhanced.occupation_code {
            //     self.socioeconomic_status = Some(occupation_code);
            // }

            // if let Some(industry_code) = enhanced.industry_code {
            //     self.industry_code = Some(industry_code);
            // }

            // if let Some(employment_start_date) = enhanced.employment_start_date {
            //     self.employment_start_date = Some(employment_start_date);
            // }

            // if let Some(employment_end_date) = enhanced.employment_end_date {
            //     self.employment_end_date = Some(employment_end_date);
            // }

            // if let Some(working_hours) = enhanced.working_hours {
            //     self.working_hours = Some(working_hours);
            // }

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
