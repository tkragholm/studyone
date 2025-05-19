//! Example demonstrating direct deserialization to Individual models
//!
//! This example shows how to use the `DirectIndividualDeserializer` to load records
//! directly into Individual models without intermediate registry-specific structs.

use arrow::array::Array;
use par_reader::registry::direct_deserializer::DirectIndividualDeserializer;
use std::path::Path;

/// Run the direct deserializer example
pub fn main() {
    println!("Running direct deserialization example");

    // Create a direct deserializer for the VNDS registry
    let deserializer = DirectIndividualDeserializer::new("VNDS");
    println!("Created direct deserializer for VNDS registry");

    // Path to the VNDS Parquet file
    let parquet_path = Path::new("/home/tkragholm/generated_data/parquet/vnds/202209.parquet");
    println!("Loading data from: {parquet_path:?}");

    // Check the actual date format in the file
    match par_reader::loader::read_parquet(parquet_path, None, None) {
        Ok(debug_batches) => {
            if let Some(batch) = debug_batches.first() {
                if let Ok(date_idx) = batch.schema().index_of("HAEND_DATO") {
                    let date_col = batch.column(date_idx);
                    if let Some(string_array) = date_col
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        println!("First 5 date values from HAEND_DATO column:");
                        for i in 0..std::cmp::min(5, string_array.len()) {
                            if string_array.is_null(i) {
                                println!("  [{i}]: NULL");
                            } else {
                                println!("  [{}]: '{}'", i, string_array.value(i));
                            }
                        }
                    } else {
                        println!("HAEND_DATO is not a string column");
                    }
                } else {
                    println!("HAEND_DATO column not found");
                }
            }
        }
        Err(err) => {
            println!("Error reading Parquet file for debug: {err}");
        }
    }

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

                // Let's manually create a few Individual objects to see if that works
                println!("Creating individuals manually from the raw data:");
                let mut manual_individuals = Vec::new();
                
                for row in 0..std::cmp::min(5, batch.num_rows()) {
                    let mut individual = par_reader::models::core::Individual::default();
                    
                    // Extract PNR
                    if let Ok(pnr_idx) = batch.schema().index_of("PNR") {
                        let pnr_col = batch.column(pnr_idx);
                        if let Some(string_array) = pnr_col.as_any().downcast_ref::<arrow::array::StringArray>() {
                            if !string_array.is_null(row) {
                                individual.pnr = string_array.value(row).to_string();
                            }
                        }
                    }
                    
                    // Extract Event Type
                    if let Ok(event_idx) = batch.schema().index_of("INDUD_KODE") {
                        let event_col = batch.column(event_idx);
                        if let Some(string_array) = event_col.as_any().downcast_ref::<arrow::array::StringArray>() {
                            if !string_array.is_null(row) {
                                individual.event_type = Some(string_array.value(row).to_string());
                            }
                        }
                    }
                    
                    // Extract Event Date
                    if let Ok(date_idx) = batch.schema().index_of("HAEND_DATO") {
                        let date_col = batch.column(date_idx);
                        if let Some(string_array) = date_col.as_any().downcast_ref::<arrow::array::StringArray>() {
                            if !string_array.is_null(row) {
                                let date_str = string_array.value(row);
                                match chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                                    Ok(date) => {
                                        individual.event_date = Some(date);
                                        println!("Manually set event_date to Some({date}) for row {row}");
                                    },
                                    Err(err) => println!("Failed to parse date manually: {err}")
                                }
                            }
                        }
                    }
                    
                    manual_individuals.push(individual);
                }
                
                // Print the manually created individuals
                println!("Manually created individuals:");
                for (i, individual) in manual_individuals.iter().enumerate() {
                    println!(
                        "[{}] PNR: {}, Event Type: {:?}, Event Date: {:?}",
                        i + 1,
                        individual.pnr,
                        individual.event_type,
                        individual.event_date
                    );
                }
                
                // Now deserialize directly to Individual models using the deserializer
                println!("\nUsing the deserializer:");
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
                                "[{}] PNR: {}, Event Type: {:?}, Event Date: {:?}",
                                i + 1,
                                individual.pnr,
                                individual.event_type,
                                individual.event_date
                            );
                        }

                        // Count event types
                        let mut event_types = std::collections::HashMap::new();
                        for individual in &individuals {
                            if let Some(event_type) = &individual.event_type {
                                *event_types.entry(event_type.clone()).or_insert(0) += 1;
                            }
                        }

                        println!("\nEvent Type Distribution:");
                        for (event_type, count) in event_types {
                            println!("  {event_type}: {count} records");
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
