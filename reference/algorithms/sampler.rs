//! Sampling algorithms for the IDS-RS library
//!
//! This module provides algorithms for sampling data from various sources.

use crate::error::{IdsError, Result};
use arrow::array::ArrayRef;
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};

/// Sample a specific number of rows from a set of record batches
///
/// This function performs sampling without replacement, drawing a specific
/// number of rows from the input record batches. If `sample_count` is greater
/// than the total number of rows available, all rows will be returned.
///
/// # Arguments
/// * `batches` - A vector of `RecordBatch` objects to sample from
/// * `sample_count` - The number of rows to sample
/// * `seed` - Optional seed for the random number generator
///
/// # Returns
/// A new vector of `RecordBatch` objects containing the sampled rows
pub fn sample_records(
    batches: &[RecordBatch],
    sample_count: usize,
    seed: Option<u64>,
) -> Result<Vec<RecordBatch>> {
    // If no batches, return empty result
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    // Count total rows
    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    // If sample count exceeds total rows, return all records
    if sample_count >= total_rows {
        return Ok(batches.to_vec());
    }

    // Create a vector of all record indices across all batches
    let mut all_indices: Vec<(usize, usize)> = Vec::with_capacity(total_rows);
    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            all_indices.push((batch_idx, row_idx));
        }
    }

    // Create RNG with optional seed
    let mut rng = match seed {
        Some(seed_value) => StdRng::seed_from_u64(seed_value),
        None => StdRng::from_os_rng(),
    };

    // Randomly sample indices
    all_indices.shuffle(&mut rng);
    let sampled_indices = &all_indices[0..sample_count];

    // Group sampled indices by batch
    let mut batch_indices: Vec<Vec<usize>> = vec![Vec::new(); batches.len()];
    for (batch_idx, row_idx) in sampled_indices {
        batch_indices[*batch_idx].push(*row_idx);
    }

    // Sample each batch separately
    let mut result_batches = Vec::new();
    for (batch_idx, indices) in batch_indices.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }

        // Sort indices for better performance
        let mut sorted_indices = indices.clone();
        sorted_indices.sort_unstable();

        // Create the array for the take kernel
        let indices_array =
            arrow::array::UInt64Array::from_iter_values(sorted_indices.iter().map(|i| *i as u64));

        // Sample each column from the batch
        let batch = &batches[batch_idx];
        let sampled_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| {
                take(col, &indices_array, None)
                    .map_err(|e| IdsError::Data(format!("Error sampling column: {e}")))
            })
            .collect::<Result<_>>()?;

        // Create a new record batch with the sampled columns
        let sampled_batch = RecordBatch::try_new(batch.schema(), sampled_columns)
            .map_err(|e| IdsError::Data(format!("Error creating sampled batch: {e}")))?;

        result_batches.push(sampled_batch);
    }

    Ok(result_batches)
}
