//! Test binary for LPR v2 data using the RegistryTrait
//!
//! This binary tests the use of the procedural macros to define LPR registry types.

use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use par_reader::RegistryTrait;
use par_reader::registry::trait_deserializer::RegistryDeserializer;
use par_reader::{Result, error};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Read a Parquet file into RecordBatches
fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    // Open the file
    let file = File::open(path).map_err(|e| {
        error::Error::IoError(format!("Failed to open file {}: {}", path.display(), e))
    })?;

    // Create a ParquetFileReader
    let file_reader = SerializedFileReader::new(file)
        .map_err(|e| error::Error::ParquetError(format!("Failed to read parquet file: {}", e)))?;

    // Create batch reader builder
    let mut batch_reader_builder = ParquetRecordBatchReaderBuilder::try_new(file_reader)
        .map_err(|e| error::Error::ParquetError(format!("Failed to create batch reader: {}", e)))?;

    // Set batch size to 1024 rows
    batch_reader_builder = batch_reader_builder.with_batch_size(1024);

    // Create batch reader
    let batch_reader = batch_reader_builder
        .build()
        .map_err(|e| error::Error::ParquetError(format!("Failed to build batch reader: {}", e)))?;

    // Collect batches
    let mut batches = Vec::new();
    for maybe_batch in batch_reader {
        let batch = maybe_batch
            .map_err(|e| error::Error::ParquetError(format!("Failed to read batch: {}", e)))?;
        batches.push(batch);
    }

    Ok(batches)
}

// LPR ADM Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(name = "LPR_ADM", description = "LPR Admission registry")]
pub struct LprAdmRegistry {
    // Core identification fields
    #[field(name = "PNR")]
    pub pnr: String,

    // Admission-related fields
    #[field(name = "C_ADIAG")]
    pub action_diagnosis: Option<String>,

    #[field(name = "C_AFD")]
    pub department_code: Option<String>,

    #[field(name = "C_KOM")]
    pub municipality_code: Option<String>,

    #[field(name = "D_INDDTO")]
    pub admission_date: Option<NaiveDate>,

    #[field(name = "D_UDDTO")]
    pub discharge_date: Option<NaiveDate>,

    #[field(name = "V_ALDER")]
    pub age: Option<i32>,

    #[field(name = "V_SENGDAGE")]
    pub length_of_stay: Option<i32>,
}

// LPR DIAG Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(name = "LPR_DIAG", description = "LPR Diagnosis registry")]
pub struct LprDiagRegistry {
    // Core identification fields
    #[field(name = "PNR")]
    pub pnr: String,

    // Diagnosis fields
    #[field(name = "C_DIAG")]
    pub diagnosis_code: Option<String>,

    #[field(name = "C_DIAGTYPE")]
    pub diagnosis_type: Option<String>,

    #[field(name = "RECNUM")]
    pub record_number: Option<String>,
}

/// Helper function to process LPR data
fn process_lpr_data(
    registry_name: &str,
    parquet_path: &Path,
    deserializer: &Arc<dyn RegistryDeserializer>,
) {
    if !parquet_path.exists() {
        println!("Parquet file not found: {:?}", parquet_path);
        println!("Skipping {} processing", registry_name);
        return;
    }

    println!("Loading data from: {:?}", parquet_path);

    // Load the Parquet file
    match read_parquet(parquet_path) {
        Ok(batches) => {
            println!("Successfully loaded {} record batches", batches.len());

            // Process each batch
            for (i, batch) in batches.iter().enumerate() {
                println!("Processing batch {} with {} rows", i + 1, batch.num_rows());

                // Print batch schema
                println!("Batch schema: {:?}", batch.schema());

                // Print field extractors
                println!("Checking extractors in the deserializer:");
                for extractor in deserializer.field_extractors() {
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

                // Deserialize the batch
                match deserializer.deserialize_batch(batch) {
                    Ok(records) => {
                        println!("Successfully deserialized {} records", records.len());

                        // Print a few records for debugging
                        let limit = std::cmp::min(5, records.len());
                        if limit > 0 {
                            println!("First {limit} records:");
                            for (j, record) in records.iter().take(limit).enumerate() {
                                println!("[{}] Record: {:?}", j + 1, record);
                            }
                        } else {
                            println!("No records found in batch");
                        }
                    }
                    Err(err) => {
                        eprintln!("Error deserializing batch: {}", err);
                    }
                }

                // Only process the first batch for brevity
                break;
            }
        }
        Err(err) => {
            eprintln!("Error loading Parquet file: {}", err);
        }
    }
}

fn main() -> Result<()> {
    println!("Testing LPR registry trait implementation");

    // Create deserializers for both registry types
    let adm_deserializer = LprAdmRegistryDeserializer::new();
    let diag_deserializer = LprDiagRegistryDeserializer::new();

    // Print deserializer info
    println!(
        "Created deserializer for {} registry",
        adm_deserializer.inner.registry_type()
    );
    println!(
        "Created deserializer for {} registry",
        diag_deserializer.inner.registry_type()
    );

    // Path to the LPR Parquet files (update with actual paths)
    let adm_parquet_path =
        Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_adm/2000.parquet");
    let diag_parquet_path =
        Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_diag/2000.parquet");

    // Process LPR ADM data if the file exists
    println!("\nProcessing LPR ADM data...");
    process_lpr_data("LPR_ADM", adm_parquet_path, &adm_deserializer.inner);

    // Process LPR DIAG data if the file exists
    println!("\nProcessing LPR DIAG data...");
    process_lpr_data("LPR_DIAG", diag_parquet_path, &diag_deserializer.inner);

    Ok(())
}
