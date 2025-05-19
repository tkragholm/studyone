//! Test the Individual.from_batch method
//!
//! This example tests if we can convert arrow record batches to Individual objects

use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, Int32Array, Date32Array};
use arrow::datatypes::{Field, Schema, DataType};
use arrow::record_batch::RecordBatch;

use par_reader::models::core::individual::Individual;
use par_reader::registry::factory;
use par_reader::utils::io::parquet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test with a simple manually created batch
    println!("Testing with manually created batch:");
    
    // Create a simple schema
    let schema = Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("KOEN", DataType::Utf8, true),
        Field::new("AAR", DataType::Int32, true),
    ]);
    
    // Create arrays
    let pnr_array = StringArray::from(vec!["1234567890", "0987654321", "1122334455"]);
    let gender_array = StringArray::from(vec![Some("M"), Some("F"), None]);
    let year_array = Int32Array::from(vec![Some(2020), Some(2021), Some(2022)]);
    
    // Create batch
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(pnr_array),
            Arc::new(gender_array),
            Arc::new(year_array),
        ],
    )?;
    
    println!("Batch schema: {}", batch.schema());
    println!("Batch row count: {}", batch.num_rows());
    
    // Try to convert to Individuals
    match Individual::from_batch(&batch) {
        Ok(individuals) => {
            println!("Converted {} individuals:", individuals.len());
            for ind in individuals {
                println!("  PNR: {}, Gender: {:?}", ind.pnr, ind.gender);
            }
        }
        Err(e) => {
            println!("Error converting batch: {}", e);
        }
    }
    
    // Now test with a real file
    println!("\nTesting with file from disk:");
    
    // Find an AKM file
    let akm_dir = Path::new("/home/tkragholm/generated_data/parquet/akm");
    let akm_files = parquet::find_parquet_files(akm_dir)?;
    
    if akm_files.is_empty() {
        println!("No AKM files found!");
    } else {
        let test_file = &akm_files[0];
        println!("Using file: {}", test_file.display());
        
        // Create the AKM loader
        let registry_loader = factory::registry_from_name("akm")?;
        
        // Load batches
        let batches = registry_loader.load(test_file, None)?;
        
        if batches.is_empty() {
            println!("No batches loaded!");
        } else {
            let first_batch = &batches[0];
            println!("First batch row count: {}", first_batch.num_rows());
            println!("First batch schema: {}", first_batch.schema());
            
            // Try to convert to Individuals
            match Individual::from_batch(first_batch) {
                Ok(individuals) => {
                    println!("Converted {} individuals:", individuals.len());
                    if !individuals.is_empty() {
                        let sample = &individuals[0];
                        println!("  Sample - PNR: {}, Gender: {:?}", sample.pnr, sample.gender);
                    }
                }
                Err(e) => {
                    println!("Error converting batch: {}", e);
                }
            }
        }
    }
    
    Ok(())
}