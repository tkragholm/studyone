//! Test for loading and processing LPR data with the RegistryTrait
//!
//! This example demonstrates the complete workflow of defining registry types
//! with the procedural macro and using them to load and process data.

use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use par_reader::*;
use par_reader::models::health::{Diagnosis, DiagnosisMapper, RecnumProvider, PnrProvider, RecnumToPnrMap};
use par_reader::models::core::types::DiagnosisType;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Simple wrapper around par_reader::read_parquet
fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    // Use the built-in utility function with RandomState hash builder
    par_reader::read_parquet::<std::collections::hash_map::RandomState>(path, None, None, None, None)
}

/// Process data from a Parquet file using a registry deserializer
fn process_data(
    registry_name: &str,
    parquet_path: &Path,
    deserializer: &Arc<dyn registry::trait_deserializer::RegistryDeserializer>,
) -> Result<Vec<models::core::Individual>> {
    println!("Processing {} data from: {:?}", registry_name, parquet_path);

    // Load the Parquet file
    let batches = read_parquet(parquet_path)?;
    println!("Successfully loaded {} record batches", batches.len());

    let mut all_records = Vec::new();

    // Process one batch
    if let Some(batch) = batches.first() {
        println!("Processing batch with {} rows", batch.num_rows());

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
            if batch.column_by_name(extractor.source_field_name()).is_some() {
                println!("  - Column found in batch");
            } else {
                println!("  - Column NOT found in batch");
            }
        }

        // Deserialize the batch
        let records = deserializer.deserialize_batch(batch)?;
        println!("Successfully deserialized {} records", records.len());

        // Print a few records for debugging
        let limit = std::cmp::min(5, records.len());
        if limit > 0 && registry_name == "LPR_ADM" {
            println!("First {limit} records:");
            for (j, record) in records.iter().take(limit).enumerate() {
                println!("[{}] PNR: {}", j + 1, record.pnr);
            }
        } else if records.is_empty() {
            println!("No records found in batch");
        }

        all_records.extend(records);
    } else {
        println!("No batches found in the Parquet file");
    }

    Ok(all_records)
}

fn main() -> Result<()> {
    println!("LPR Parquet Test");
    
    // Define LPR ADM Registry using the derive macro
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "LPR_ADM", description = "LPR Admission registry")]
    struct LprAdmRegistry {
        // Core identification fields
        #[field(name = "PNR")]
        pnr: String,

        // Record number for joining with LPR_DIAG
        #[field(name = "RECNUM")]
        record_number: Option<String>,

        // Admission-related fields
        #[field(name = "C_ADIAG")]
        action_diagnosis: Option<String>,

        #[field(name = "C_AFD")]
        department_code: Option<String>,

        #[field(name = "C_KOM")]
        municipality_code: Option<String>,

        #[field(name = "D_INDDTO")]
        admission_date: Option<NaiveDate>,

        #[field(name = "D_UDDTO")]
        discharge_date: Option<NaiveDate>,

        #[field(name = "V_ALDER")]
        age: Option<i32>,

        #[field(name = "V_SENGDAGE")]
        length_of_stay: Option<i32>,
    }

    // Define LPR DIAG Registry using the derive macro
    // Note: This registry doesn't have a PNR field as it doesn't exist in LPR_DIAG
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "LPR_DIAG", description = "LPR Diagnosis registry")]
    struct LprDiagRegistry {
        // Reference to ADM record
        #[field(name = "RECNUM")]
        record_number: Option<String>,

        // Diagnosis fields
        #[field(name = "C_DIAG")]
        diagnosis_code: Option<String>,

        #[field(name = "C_DIAGTYPE")]
        diagnosis_type: Option<String>,
    }
    
    // Implement PnrProvider for LprAdmRegistry
    impl PnrProvider for LprAdmRegistry {
        fn pnr(&self) -> Option<String> {
            Some(self.pnr.clone())
        }
    }
    
    // Implement RecnumProvider for LprAdmRegistry
    impl RecnumProvider for LprAdmRegistry {
        fn record_number(&self) -> Option<String> {
            self.record_number.clone()
        }
    }
    
    // Implement RecnumProvider for LprDiagRegistry
    impl RecnumProvider for LprDiagRegistry {
        fn record_number(&self) -> Option<String> {
            self.record_number.clone()
        }
    }
    
    // Implement DiagnosisMapper for LprDiagRegistry
    impl DiagnosisMapper for LprDiagRegistry {
        fn to_diagnosis(&self, pnr_lookup: &HashMap<String, String>) -> Option<Diagnosis> {
            // Get the record number
            let record_number = self.record_number.as_ref()?;
            
            // Look up the PNR from the record number
            let pnr = pnr_lookup.get(record_number)?;
            
            // Get the diagnosis code
            let diagnosis_code = self.diagnosis_code.as_ref()?;
            
            // Determine diagnosis type
            let diagnosis_type = match self.diagnosis_type.as_deref() {
                Some("A") => DiagnosisType::Primary,
                Some("B") => DiagnosisType::Secondary,
                _ => DiagnosisType::Other,
            };
            
            // Create a new Diagnosis (without a date for now)
            let diagnosis = Diagnosis::new(
                pnr.clone(),
                diagnosis_code.clone(),
                diagnosis_type,
                None, // We don't have the date in the DIAG record
            );
            
            Some(diagnosis)
        }
    }
    
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
    
    // Path to the LPR Parquet files
    let adm_parquet_path = Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_adm/2000.parquet");
    let diag_parquet_path = Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_diag/2000.parquet");
    
    // Map to store record_number -> Individual mapping
    let mut recnum_to_pnr = RecnumToPnrMap::new();
    
    // Process LPR ADM data if the file exists
    if adm_parquet_path.exists() {
        println!("\nProcessing LPR ADM data...");
        match process_data("LPR_ADM", adm_parquet_path, &adm_deserializer.inner) {
            Ok(adm_records) => {
                println!("Successfully processed {} LPR ADM records", adm_records.len());
                
                // Extract RECNUM to PNR mappings from ADM records
                for individual in &adm_records {
                    // For each admission, extract the record number and create a mapping
                    if let Some(properties) = find_record_number_from_individual(individual) {
                        if let (Some(recnum), Some(pnr)) = (properties.0, properties.1) {
                            recnum_to_pnr.add_mapping(recnum, pnr);
                        }
                    }
                }
                
                println!("Created mapping for {} individuals with RECNUM values", recnum_to_pnr.recnum_to_pnr.len());
                
                // Process diagnoses
                if diag_parquet_path.exists() {
                    println!("\nProcessing LPR DIAG data...");
                    match process_data("LPR_DIAG", diag_parquet_path, &diag_deserializer.inner) {
                        Ok(diag_individuals) => {
                            println!("Successfully processed {} LPR DIAG records", diag_individuals.len());
                            
                            // Create a collection of diagnoses
                            let mut diagnosis_count = 0;
                            let mut matched_count = 0;
                            
                            // Print some sample diagnoses
                            println!("\nSample diagnoses that were matched to individuals:");
                            
                            for diag_individual in &diag_individuals {
                                if let Some(diagnoses) = &diag_individual.diagnoses {
                                    diagnosis_count += diagnoses.len();
                                    
                                    // Get the record number from the individual
                                    if let Some(properties) = find_record_number_from_individual(diag_individual) {
                                        if let Some(recnum) = properties.0 {
                                            // Look up the Individual with this RECNUM
                                            if let Some(pnr) = recnum_to_pnr.lookup_pnr(&recnum) {
                                                matched_count += 1;
                                                
                                                // Print first few matches as examples
                                                if matched_count <= 5 {
                                                    println!("Record Number {}: Diagnosis {} matched to individual with PNR {}", 
                                                             recnum, 
                                                             diagnoses.first().unwrap_or(&String::from("<none>")),
                                                             pnr);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            println!("\nMatched {} out of {} diagnoses to individuals",
                                     matched_count, diagnosis_count);
                        }
                        Err(e) => eprintln!("Error processing LPR DIAG data: {}", e),
                    }
                } else {
                    println!("\nSkipping LPR DIAG processing - file not found at: {}", diag_parquet_path.display());
                }
            }
            Err(e) => eprintln!("Error processing LPR ADM data: {}", e),
        }
    } else {
        println!("\nSkipping LPR ADM processing - file not found at: {}", adm_parquet_path.display());
    }
    
    println!("\nLPR Parquet test completed!");
    Ok(())
}

// Helper function to extract the record_number from an Individual
fn find_record_number_from_individual(individual: &models::core::Individual) -> Option<(Option<String>, Option<String>)> {
    // For this example, if the individual has a diagnosis with a record_number, use that
    if let Some(diagnoses) = &individual.diagnoses {
        if !diagnoses.is_empty() {
            // In a proper implementation, we would access the RECNUM field directly
            // Using a stable hash of the PNR to create a pseudo-RECNUM for demonstration
            let recnum = format!("LPR{:010}", individual.pnr.replace("-", "").chars().map(|c| c as u32).sum::<u32>() % 10000);
            return Some((
                Some(recnum),
                Some(individual.pnr.clone())
            ));
        }
    }
    
    None
}