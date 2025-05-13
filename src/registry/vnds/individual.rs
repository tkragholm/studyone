//! VNDS registry trait implementations for Individual
//!
//! This module contains the implementation of `VndsRegistry` for the Individual model.

use crate::RecordBatch;
use crate::common::traits::VndsRegistry;
use crate::error::Result;
use crate::models::core::Individual;

impl VndsRegistry for Individual {
    fn from_vnds_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::vnds::trait_deserializer_macro::deserialize_row(batch, row)
    }

    fn from_vnds_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::vnds::trait_deserializer_macro::deserialize_batch(batch)
    }

    fn enhance_with_migration_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // With our unified schema approach, enhancement is handled automatically by the deserializer
        // The trait-based deserializer uses registry traits and field mappings from the unified schema

        // Create a temporary Individual using the deserializer
        if let Some(enhanced) =
            crate::registry::vnds::trait_deserializer_macro::deserialize_row(batch, row)?
        {
            // Copy migration-specific fields from the enhanced Individual to this one
            if let Some(emigration_date) = enhanced.emigration_date {
                self.emigration_date = Some(emigration_date);
            }

            if let Some(immigration_date) = enhanced.immigration_date {
                self.immigration_date = Some(immigration_date);
            }

            if let Some(emigration_country) = enhanced.emigration_country {
                self.emigration_country = Some(emigration_country);
            }

            if let Some(immigration_country) = enhanced.immigration_country {
                self.immigration_country = Some(immigration_country);
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
