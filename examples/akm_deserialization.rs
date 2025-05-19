//! Example demonstrating direct deserialization of AKM registry to Individual models
//!
//! This example shows how to use the `DirectIndividualDeserializer` to load AKM records
//! directly into Individual models without intermediate registry-specific structs.

use par_reader::registry::direct_deserializer::DirectIndividualDeserializer;
use std::path::Path;

/// Run the AKM direct deserializer example
pub fn main() {
    println!("Running AKM direct deserialization example");

    // Create a direct deserializer for the AKM registry
    let deserializer = DirectIndividualDeserializer::new("AKM");
    println!("Created direct deserializer for AKM registry");

    // Path to the AKM Parquet file
    // Update this path to the actual location of your AKM parquet file
    let parquet_path = Path::new("/home/tkragholm/generated_data/parquet/akm/2022.parquet");
    println!("Loading data from: {parquet_path:?}");

    // Load the Parquet file
    match par_reader::loader::read_parquet(parquet_path, None, None) {
        Ok(batches) => {
            eprintln!("Successfully loaded {} record batches", batches.len());

            // Process the first batch
            if let Some(batch) = batches.first() {
                eprintln!("Processing batch with {} rows", batch.num_rows());

                // Print information about field extractors
                eprintln!("Field extractors used by the deserializer:");
                for extractor in deserializer.field_extractors() {
                    eprintln!(
                        "  Extractor maps {} -> {}",
                        extractor.source_field_name(),
                        extractor.target_field_name()
                    );
                }

                // Now deserialize directly to Individual models using the deserializer
                match deserializer.deserialize_batch(batch) {
                    Ok(individuals) => {
                        eprintln!(
                            "Successfully deserialized {} individuals",
                            individuals.len()
                        );

                        // Print a sample of the results
                        let limit = std::cmp::min(5, individuals.len());
                        eprintln!("First {limit} individuals:");

                        for (i, individual) in individuals.iter().take(limit).enumerate() {
                            eprintln!(
                                "[{}] PNR: {}, Socioeconomic Status: {:?}",
                                i + 1,
                                individual.pnr,
                                individual.socioeconomic_status
                            );
                        }

                        // Count socioeconomic status distribution
                        let mut status_counts = std::collections::HashMap::new();
                        let mut individuals_with_status = 0;
                        let mut individuals_without_status = 0;

                        for individual in &individuals {
                            if let Some(status) = individual.socioeconomic_status {
                                *status_counts.entry(status).or_insert(0) += 1;
                                individuals_with_status += 1;
                            } else {
                                individuals_without_status += 1;
                            }
                        }

                        eprintln!("\nSocioeconomic Status Summary:");
                        eprintln!("  Individuals with status: {individuals_with_status}");
                        eprintln!("  Individuals without status: {individuals_without_status}");

                        eprintln!("\nSocioeconomic Status Distribution:");
                        let mut sorted_statuses: Vec<_> = status_counts.iter().collect();
                        sorted_statuses.sort_by_key(|&(status, _)| *status);

                        for (status, count) in sorted_statuses {
                            eprintln!(
                                "  Status {status}: {count} records ({:.2}%)",
                                (f64::from(*count) / individuals.len() as f64) * 100.0
                            );
                        }
                    }
                    Err(err) => {
                        eprintln!("Error deserializing batch: {err}");
                    }
                }
            }
        }
        Err(err) => {
            eprintln!("Error loading Parquet file: {err}");
        }
    }
}
