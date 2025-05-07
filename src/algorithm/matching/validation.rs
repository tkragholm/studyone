//! Validation functions for the matching algorithm
//!
//! This module contains functions for validating input data before matching.

use crate::algorithm::matching::criteria::MatchingConfig;
use crate::error::{ParquetReaderError, Result};
use arrow::record_batch::RecordBatch;

/// Validate that the input batches have the required columns
pub fn validate_batches(
    cases: &RecordBatch,
    controls: &RecordBatch,
    config: &MatchingConfig,
) -> Result<()> {
    let required_columns = ["PNR", "FOED_DAG"];

    for column in &required_columns {
        if cases.schema().field_with_name(column).is_err() {
            return Err(ParquetReaderError::ValidationError(format!(
                "Cases batch missing required column: {column}"
            ))
            .into());
        }

        if controls.schema().field_with_name(column).is_err() {
            return Err(ParquetReaderError::ValidationError(format!(
                "Controls batch missing required column: {column}"
            ))
            .into());
        }
    }

    // Check for KOEN if gender matching is required
    if config.criteria.require_same_gender {
        if cases.schema().field_with_name("KOEN").is_err() {
            return Err(ParquetReaderError::ValidationError(
                "Cases batch missing KOEN column required for gender matching".to_string(),
            )
            .into());
        }

        if controls.schema().field_with_name("KOEN").is_err() {
            return Err(ParquetReaderError::ValidationError(
                "Controls batch missing KOEN column required for gender matching".to_string(),
            )
            .into());
        }
    }

    // Additional validation for other matching criteria would go here

    Ok(())
}