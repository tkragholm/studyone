//! Example demonstrating direct use of Individual properties
//!
//! This example shows how to directly set properties on the Individual model
//! without using registry conversion.

use arrow::array::Array;
use chrono::NaiveDate;
use par_reader::models; // Import everything from par_reader
use std::path::Path;
use std::any::Any;

/// Run the direct Individual properties example
pub fn main() {
    println!("Running Individual properties example");

    // Path to the VNDS Parquet file
    let parquet_path = Path::new("/home/tkragholm/generated_data/parquet/vnds/202209.parquet");
    println!("Loading data from: {parquet_path:?}");

    // Load the Parquet file using the crate's utility function
    match par_reader::loader::read_parquet(parquet_path, None, None) {
        Ok(batches) => {
            println!("Successfully loaded {} record batches", batches.len());

            // Process the first batch
            if let Some(batch) = batches.first() {
                println!("Processing batch with {} rows", batch.num_rows());

                // Print batch schema to see available columns
                println!("Batch schema: {:?}", batch.schema());

                let mut individuals = Vec::new();

                // Process the first few rows
                let num_rows = std::cmp::min(5, batch.num_rows());
                for row in 0..num_rows {
                    // Create a new Individual for each row
                    let mut individual = models::core::Individual::default();

                    // Extract PNR
                    if let Ok(pnr_idx) = batch.schema().index_of("PNR") {
                        let pnr_array = batch.column(pnr_idx);
                        if let Some(string_array) = pnr_array.as_any().downcast_ref::<arrow::array::StringArray>() {
                            if !string_array.is_null(row) {
                                individual.pnr = string_array.value(row).to_string();
                            }
                        }
                    }

                    // Extract Event Type (INDUD_KODE)
                    if let Ok(event_type_idx) = batch.schema().index_of("INDUD_KODE") {
                        let event_array = batch.column(event_type_idx);
                        if let Some(string_array) = event_array.as_any().downcast_ref::<arrow::array::StringArray>() {
                            if !string_array.is_null(row) {
                                let value = string_array.value(row).to_string();
                                println!("Setting event_type for row {row}: {value}");
                                // Set the value both directly and in properties
                                individual.event_type = Some(value.clone());
                                
                                // Also set in properties to illustrate the dual storage
                                if individual.properties.is_none() {
                                    individual.properties = Some(std::collections::HashMap::new());
                                }
                                if let Some(props) = &mut individual.properties {
                                    props.insert("event_type".to_string(), 
                                        Box::new(Some(value)) as Box<dyn Any + Send + Sync>);
                                }
                            }
                        }
                    }

                    // Extract Event Date (HAEND_DATO)
                    if let Ok(date_idx) = batch.schema().index_of("HAEND_DATO") {
                        let date_array = batch.column(date_idx);
                        if let Some(string_array) = date_array.as_any().downcast_ref::<arrow::array::StringArray>() {
                            if !string_array.is_null(row) {
                                let date_str = string_array.value(row);
                                println!("Parsing date string: '{date_str}' for field 'HAEND_DATO'");
                                
                                // Parse the date
                                if let Ok(parsed_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                                    println!("Successfully parsed date: Some({parsed_date})");
                                    // Set the value both directly and in properties
                                    individual.event_date = Some(parsed_date);
                                    
                                    // Also set in properties
                                    if individual.properties.is_none() {
                                        individual.properties = Some(std::collections::HashMap::new());
                                    }
                                    if let Some(props) = &mut individual.properties {
                                        props.insert("event_date".to_string(), 
                                            Box::new(Some(parsed_date)) as Box<dyn Any + Send + Sync>);
                                    }
                                }
                            }
                        }
                    }

                    individuals.push(individual);
                }

                // Print out the Individuals we've created
                println!("Created {} Individual records", individuals.len());
                for (i, individual) in individuals.iter().enumerate() {
                    println!(
                        "[{}] PNR: {}, Event Type: {:?}, Event Date: {:?}",
                        i + 1,
                        individual.pnr,
                        individual.event_type,
                        individual.event_date
                    );
                }

                // Show how to access properties via properties map
                println!("\nAccessing fields via properties map:");
                for (i, individual) in individuals.iter().enumerate() {
                    let event_type_prop = individual.properties()
                        .and_then(|props| props.get("event_type"))
                        .and_then(|v| v.downcast_ref::<Option<String>>())
                        .and_then(|v| v.as_ref().map(std::string::String::as_str));
                    
                    let event_date_prop = individual.properties()
                        .and_then(|props| props.get("event_date"))
                        .and_then(|v| v.downcast_ref::<Option<NaiveDate>>())
                        .and_then(|v| v.as_ref());
                    
                    println!(
                        "[{}] From properties map - Event Type: {:?}, Event Date: {:?}",
                        i + 1,
                        event_type_prop,
                        event_date_prop
                    );
                }
            }
        }
        Err(err) => {
            eprintln!("Error loading Parquet file: {err}");
        }
    }
}
