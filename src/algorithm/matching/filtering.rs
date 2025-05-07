//! Filtering utilities for the matching algorithm
//!
//! This module provides functions for filtering record batches based on indices.

use crate::error::{ParquetReaderError, Result};
use arrow::array::BooleanArray;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Filter a `RecordBatch` by row indices
///
/// # Arguments
/// * `batch` - The record batch to filter
/// * `indices` - The indices of rows to keep
///
/// # Returns
/// A filtered RecordBatch containing only the specified rows
///
/// # Errors
/// Returns an error if filtering fails
pub fn filter_batch_by_indices(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    // Create a boolean mask for the selected rows
    let mut mask = vec![false; batch.num_rows()];
    for &idx in indices {
        if idx < mask.len() {
            mask[idx] = true;
        } else {
            return Err(ParquetReaderError::ValidationError(format!(
                "Index out of bounds: {} >= {}",
                idx,
                mask.len()
            ))
            .into());
        }
    }

    let bool_array = BooleanArray::from(mask);

    // Apply the mask to all columns
    let filtered_columns: Result<Vec<Arc<dyn arrow::array::Array>>> = batch
        .columns()
        .iter()
        .map(|col| {
            compute::filter(col, &bool_array).map_err(|e| {
                ParquetReaderError::ValidationError(format!("Failed to filter column: {e}"))
                    .into()
            })
        })
        .collect();

    // Create the filtered RecordBatch
    RecordBatch::try_new(batch.schema(), filtered_columns?).map_err(|e| {
        ParquetReaderError::ValidationError(format!("Failed to create filtered batch: {e}"))
            .into()
    })
}