//! LPR registry trait implementations for Individual
//!
//! This module contains the implementation of LprRegistry for the Individual model.

use crate::RecordBatch;
use crate::common::traits::LprRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::types::Gender;
use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;

impl LprRegistry for Individual {
    fn from_lpr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR from registry data
        let pnr_col = crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array =
                crate::utils::array_utils::downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual with just the PNR
        let individual = Individual::new(pnr, Gender::Unknown, None);
        Ok(Some(individual))
    }

    fn from_lpr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // For now, use row-by-row deserialization
        // This would be enhanced with a proper LPR deserializer later
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_lpr_record(batch, row)? {
                individuals.push(individual);
            }
        }
        Ok(individuals)
    }
}
