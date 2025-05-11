use crate::error::Result;
use crate::examples::parrallel_loader::load_parquet_files_parallel_with_filter;
use crate::filter_expression::col;
use crate::models::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::registry::bef::deserializer;
use arrow::array::Array;
use arrow::compute::kernels::take;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;

/// Run an example that loads BEF files, filters by date range, and converts to Individual models
///
/// This example demonstrates:
/// 1. Loading parquet files with parallel processing
/// 2. Filtering by date range
/// 3. Converting Arrow record batches to Individual domain models
///
/// # Arguments
/// * `data_dir` - The directory containing BEF parquet files
///
/// # Returns
/// The number of records processed
pub async fn run_conversion_example(data_dir: &Path) -> Result<usize> {
    println!("Loading BEF data from: {}", data_dir.display());

    // Define date range filter (1995-01-01 to 2018-12-31)
    let date_filter = col("FOED_DAG").contains(""); // We'll filter by date afterwards

    // Define columns to include - only request columns that we know exist in all files
    let column_vec = [
        "PNR".to_string(),
        "FOED_DAG".to_string(),
        "KOEN".to_string(),
        "FAR_ID".to_string(),
        "MOR_ID".to_string(),
        "CIVST".to_string(),
        "STATSB".to_string(),
        "FAMILIE_ID".to_string(),
    ];
    let columns = Some(&column_vec[..]);

    // Load BEF files in parallel
    let record_batches = load_parquet_files_parallel_with_filter(data_dir, &date_filter, columns)?;
    println!(
        "Loaded {} record batches with {} total rows",
        record_batches.len(),
        record_batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>()
    );

    // Filter by date range and convert to Individual models
    let start_date = "1995-01-01";
    let end_date = "2018-12-31";

    let mut filtered_batches: Vec<RecordBatch> = Vec::new();
    let mut individuals: Vec<Individual> = Vec::new();
    let mut total_filtered_rows = 0;

    // Process record batches to filter and convert
    for batch in &record_batches {
        // 1. Filter the batch by date range
        let date_col_idx = batch.schema().index_of("FOED_DAG").unwrap_or(1);

        if let Some(date_col) = batch
            .column(date_col_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            // Create a filtered version of this batch with matching dates
            let mut indices = Vec::new();

            for i in 0..date_col.len() {
                if !date_col.is_null(i) {
                    let date_str = date_col.value(i);
                    if date_str >= start_date && date_str <= end_date {
                        indices.push(i as u64);
                        total_filtered_rows += 1;
                    }
                }
            }

            // If there are matching rows, create a filtered batch
            if !indices.is_empty() {
                // Create a new RecordBatch with just the selected rows
                let filtered_batch =
                    take::take_record_batch(batch, &arrow::array::UInt64Array::from(indices))?;

                filtered_batches.push(filtered_batch);
            }
        }
    }

    // 2. Convert filtered batches to Individual models
    // Keep track of error types to avoid printing the same error repeatedly
    let mut seen_errors = HashSet::new();
    let mut error_count = 0;

    for (batch_idx, batch) in filtered_batches.iter().enumerate() {
        // Apply field mapping to match the expected field names
        let mapped_batch =
            match deserializer::create_mapped_batch(batch, deserializer::field_mapping()) {
                Ok(batch) => batch,
                Err(e) => {
                    let error_msg = e.to_string();
                    if seen_errors.insert(error_msg.clone()) {
                        // Only log unique errors
                        log::warn!("Failed to map batch {batch_idx}: {error_msg}");
                    }
                    error_count += 1;
                    continue;
                }
            };

        // Use SerdeIndividual to deserialize the batch
        match SerdeIndividual::from_batch(&mapped_batch) {
            Ok(serde_individuals) => {
                // Convert SerdeIndividual to regular Individual
                let batch_individuals = serde_individuals
                    .into_iter()
                    .map(SerdeIndividual::into_inner)
                    .collect::<Vec<_>>();

                // Add to our collection
                individuals.extend(batch_individuals);
            }
            Err(e) => {
                let error_msg = e.to_string();
                if seen_errors.insert(error_msg.clone()) {
                    // Only log unique errors
                    log::warn!("Failed to deserialize batch {batch_idx}: {error_msg}");
                }
                error_count += 1;
            }
        }
    }

    // If we had errors, print a summary instead of individual messages
    if error_count > 0 {
        log::warn!(
            "{} batches failed to deserialize with {} unique error types",
            error_count,
            seen_errors.len()
        );

        // Print a few examples of error types if there are multiple
        if seen_errors.len() > 1 {
            log::warn!("Error types include:");
            for (i, error) in seen_errors.iter().take(3).enumerate() {
                log::warn!("  {}. {}", i + 1, error);
            }
            if seen_errors.len() > 3 {
                log::warn!("  ... and {} more error types", seen_errors.len() - 3);
            }
        }
    }

    println!(
        "Filtered record batches by date: {} individuals with birth dates between {} and {}",
        individuals.len(),
        start_date,
        end_date
    );

    // Print some sample individuals
    if !individuals.is_empty() {
        let sample_size = std::cmp::min(5, individuals.len());
        println!("\nSample Individuals (first {sample_size}):");

        for (i, individual) in individuals.iter().take(sample_size).enumerate() {
            println!(
                "Individual {}: PNR={}, Gender={:?}, Birth date={:?}, Origin={:?}",
                i + 1,
                individual.pnr,
                individual.gender,
                individual.birth_date,
                individual.origin
            );
        }
    }

    Ok(individuals.len())
}
