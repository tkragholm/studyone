//! UDDF registry trait implementations for Individual
//!
//! This module contains the implementation of `UddfRegistry` for the Individual model.
//! This implementation uses the unified schema and trait-based deserializer.

use crate::RecordBatch;
use crate::common::traits::UddfRegistry;
use crate::error::Result;
use crate::models::core::Individual;

impl UddfRegistry for Individual {
    fn from_uddf_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the trait-based deserializer for the row
        // This uses the unified schema definition automatically
        crate::registry::uddf::trait_deserializer_macro::deserialize_row(batch, row)
    }

    fn from_uddf_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the trait-based deserializer for the batch
        // This uses the unified schema definition automatically
        crate::registry::uddf::trait_deserializer_macro::deserialize_batch(batch)
    }

    fn enhance_with_education_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // With our unified schema approach, enhancement is handled automatically by the deserializer
        // The trait-based deserializer uses registry traits and field mappings from the unified schema

        // Create a temporary Individual using the deserializer
        if let Some(enhanced) =
            crate::registry::uddf::trait_deserializer_macro::deserialize_row(batch, row)?
        {
            // Copy education-specific fields from the enhanced Individual to this one
            if let Some(institution) = enhanced.education_institution {
                self.education_institution = Some(institution);
            }

            if let Some(completion_date) = enhanced.education_completion_date {
                self.education_completion_date = Some(completion_date);
            }

            if let Some(start_date) = enhanced.education_start_date {
                self.education_start_date = Some(start_date);
            }

            if let Some(program_code) = enhanced.education_program_code {
                self.education_program_code = Some(program_code);
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
