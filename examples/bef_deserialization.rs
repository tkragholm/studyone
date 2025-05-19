//! Example demonstrating direct deserialization of BEF registry to Individual models
//!
//! This example shows how to use the `DirectIndividualDeserializer` to load BEF records
//! directly into Individual models without intermediate registry-specific structs.

use par_reader::registry::direct_deserializer::DirectIndividualDeserializer;
use std::path::Path;

/// Run the BEF direct deserializer example
pub fn main() {
    println!("Running BEF direct deserialization example");

    // Create a direct deserializer for the BEF registry
    let deserializer = DirectIndividualDeserializer::new("BEF");
    println!("Created direct deserializer for BEF registry");

    // Path to the BEF Parquet file
    // Update this path to the actual location of your BEF parquet file
    let parquet_path = Path::new("/home/tkragholm/generated_data/parquet/bef/202209.parquet");
    println!("Loading data from: {parquet_path:?}");

    // Load the Parquet file
    match par_reader::loader::read_parquet(parquet_path, None, None) {
        Ok(batches) => {
            println!("Successfully loaded {} record batches", batches.len());

            // Process the first batch
            if let Some(batch) = batches.first() {
                println!("Processing batch with {} rows", batch.num_rows());

                // Print batch schema to see available columns
                println!("Batch schema: {:?}", batch.schema());

                // Print information about field extractors
                println!("Field extractors used by the deserializer:");
                for extractor in deserializer.field_extractors() {
                    println!(
                        "  Extractor maps {} -> {}",
                        extractor.source_field_name(),
                        extractor.target_field_name()
                    );
                }

                // Now deserialize directly to Individual models using the deserializer
                match deserializer.deserialize_batch(batch) {
                    Ok(individuals) => {
                        println!(
                            "Successfully deserialized {} individuals",
                            individuals.len()
                        );

                        // Print a sample of the results
                        let limit = std::cmp::min(5, individuals.len());
                        println!("First {limit} individuals:");

                        for (i, individual) in individuals.iter().take(limit).enumerate() {
                            println!(
                                "[{}] PNR: {}, Gender: {:?}, Birth Date: {:?}",
                                i + 1,
                                individual.pnr,
                                individual.gender,
                                individual.birth_date
                            );
                            println!(
                                "    Family ID: {:?}, Mother PNR: {:?}, Father PNR: {:?}",
                                individual.family_id, individual.mother_pnr, individual.father_pnr
                            );
                            println!(
                                "    Family Size: {:?}, Household Size: {:?}, Position in Family: {:?}",
                                individual.family_size,
                                individual.household_size,
                                individual.position_in_family
                            );
                        }

                        // Calculate some statistics
                        let mut gender_counts = std::collections::HashMap::new();
                        let mut family_size_sum = 0;
                        let mut family_size_count = 0;
                        let mut has_mother = 0;
                        let mut has_father = 0;
                        let mut has_both_parents = 0;

                        for individual in &individuals {
                            if let Some(gender) = &individual.gender {
                                *gender_counts.entry(gender.clone()).or_insert(0) += 1;
                            }

                            if let Some(size) = individual.family_size {
                                family_size_sum += size;
                                family_size_count += 1;
                            }

                            if individual.mother_pnr.is_some() {
                                has_mother += 1;
                            }

                            if individual.father_pnr.is_some() {
                                has_father += 1;
                            }

                            if individual.mother_pnr.is_some() && individual.father_pnr.is_some() {
                                has_both_parents += 1;
                            }
                        }

                        println!("\nPopulation Statistics:");
                        println!("  Gender Distribution:");
                        for (gender, count) in gender_counts {
                            println!("    {gender}: {count} individuals");
                        }

                        if family_size_count > 0 {
                            println!(
                                "  Average Family Size: {:.2}",
                                f64::from(family_size_sum) / f64::from(family_size_count)
                            );
                        }

                        println!("  Parental Information:");
                        println!("    Have Mother: {has_mother} individuals");
                        println!("    Have Father: {has_father} individuals");
                        println!("    Have Both Parents: {has_both_parents} individuals");
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
