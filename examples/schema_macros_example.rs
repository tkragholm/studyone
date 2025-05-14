//! Example demonstrating the use of schema macros
//!
//! This example shows how to use the procedural macros to define registries
//! with minimal boilerplate.

use arrow::array::Array;
use chrono::NaiveDate;
use macros::RegistryTrait;
use std::path::Path;

// Now we can use the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(name = "VNDS", description = "Migration registry")]
pub struct VndsRegistry {
    // Field mappings
    #[field(name = "PNR")]
    pub pnr: String,

    #[field(name = "INDUD_KODE")]
    pub event_type: Option<String>,

    #[field(name = "HAEND_DATO")]
    pub event_date: Option<NaiveDate>,
}

/// Run the schema macros example
pub fn run_schema_macros_example() {
    println!("Running schema macros example");

    // Create a deserializer using our derive-generated code
    let deserializer = VndsRegistryDeserializer::new();

    // Print deserializer info
    println!("Created deserializer for VNDS registry");

    // Path to the VNDS Parquet file
    let parquet_path =
        Path::new("/Users/tobiaskragholm/generated_data/parquet/vnds/202209.parquet");
    println!("Loading data from: {parquet_path:?}");

    // Load the Parquet file using the crate's utility function
    match crate::examples::parrallel_loader::read_parquet(parquet_path, None, None) {
        Ok(batches) => {
            println!("Successfully loaded {} record batches", batches.len());

            // Process each batch
            for (i, batch) in batches.iter().enumerate() {
                println!("Processing batch {} with {} rows", i + 1, batch.num_rows());

                // Print batch schema to see available columns
                println!("Batch schema: {:?}", batch.schema());

                // Check if PNR column exists
                if let Ok(pnr_idx) = batch.schema().index_of("PNR") {
                    println!("PNR column found at index {pnr_idx}");
                    // Print first 5 PNR values
                    let pnr_array = batch.column(pnr_idx);
                    if let Some(string_array) = pnr_array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        println!("First 5 PNR values from raw data:");
                        for i in 0..std::cmp::min(5, string_array.len()) {
                            println!("  [{}]: {}", i, string_array.value(i));
                        }
                    }
                } else {
                    println!("PNR column not found in schema!");
                }

                // Get the schema info from the deserializer
                println!("Checking extractors in the deserializer:");
                for extractor in deserializer.inner.field_extractors() {
                    println!(
                        "  Extractor source field: {}, target field: {}",
                        extractor.source_field_name(),
                        extractor.target_field_name()
                    );
                    // Check if this field exists in the batch
                    if batch
                        .column_by_name(extractor.source_field_name())
                        .is_some()
                    {
                        println!("  - Column found in batch");
                    } else {
                        println!("  - Column NOT found in batch");
                    }
                }

                // Deserialize the batch into our custom registry type
                match deserializer.deserialize_batch(batch) {
                    Ok(records) => {
                        println!("Successfully deserialized {} records", records.len());

                        // Print the first 5 records (or fewer if there are less than 5)
                        let limit = std::cmp::min(5, records.len());
                        println!("First {limit} records:");

                        for (j, record) in records.iter().take(limit).enumerate() {
                            println!(
                                "[{}] PNR: {}, Event Type: {:?}, Event Date: {:?}",
                                j + 1,
                                record.pnr,
                                record.event_type,
                                record.event_date
                            );
                        }

                        // Show some statistics
                        let mut event_types = std::collections::HashMap::new();
                        for record in &records {
                            if let Some(event_type) = &record.event_type {
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

                // Only process the first batch for brevity
                break;
            }
        }
        Err(err) => {
            eprintln!("Error loading Parquet file: {err}");
        }
    }
}
