//! Example for LPR3 registry using DW_EK_KONTAKT as the primary identifier field.
//!
//! This example demonstrates how to use the LPR3 registry with DW_EK_KONTAKT
//! as the primary identifier field instead of PNR.

use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use par_reader::models::core::types::DiagnosisType;
use par_reader::models::health::{Diagnosis, DiagnosisMapper};
use par_reader::*;
use std::collections::HashMap;
use std::path::Path;

/// Simple wrapper around par_reader::read_parquet
fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    // Use the built-in utility function
    par_reader::read_parquet::<std::collections::hash_map::RandomState>(
        path, None, None, None, None,
    )
}

// Define a trait for providing DW_EK_KONTAKT ID from registry structs
pub trait DwEkKontaktProvider {
    fn dw_ek_kontakt(&self) -> Option<String>;
}

// Define a map for DW_EK_KONTAKT to PNR mapping
pub struct DwEkKontaktToPnrMap {
    dw_ek_kontakt_to_pnr: HashMap<String, String>,
}

impl DwEkKontaktToPnrMap {
    pub fn new() -> Self {
        Self {
            dw_ek_kontakt_to_pnr: HashMap::new(),
        }
    }

    pub fn add_mapping(&mut self, dw_ek_kontakt: String, pnr: String) {
        self.dw_ek_kontakt_to_pnr.insert(dw_ek_kontakt, pnr);
    }
}

fn main() -> Result<()> {
    println!("LPR3 Example");

    // Define LPR3 Kontakt Registry with PNR as the identifier
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "LPR3_KONTAKT", description = "LPR3 Kontakt registry", id_field = "pnr")]
    struct Lpr3KontaktRegistry {
        // Core identification fields
        #[field(name = "PNR")]
        pnr: String,

        // LPR3 kontakt ID for joining with diagnoser
        #[field(name = "DW_EK_KONTAKT")]
        dw_ek_kontakt: Option<String>,

        // Admission date
        #[field(name = "DATOTID_START")]
        start_date: Option<NaiveDate>,
    }

    // Define LPR3 Diagnoser Registry using DW_EK_KONTAKT as the identifier
    #[derive(RegistryTrait, Debug)]
    #[registry(
        name = "LPR3_DIAGNOSER",
        description = "LPR3 Diagnosis registry",
        id_field = "dw_ek_kontakt"
    )]
    struct Lpr3DiagnoserRegistry {
        // Reference to Kontakt record
        #[field(name = "DW_EK_KONTAKT")]
        dw_ek_kontakt: Option<String>,

        // Diagnosis fields
        #[field(name = "KODE")]
        diagnosis_code: Option<String>,

        #[field(name = "ART")]
        diagnosis_type: Option<String>,
    }

    // Implement PnrProvider for Lpr3KontaktRegistry
    impl par_reader::models::health::PnrProvider for Lpr3KontaktRegistry {
        fn pnr(&self) -> Option<String> {
            Some(self.pnr.clone())
        }
    }

    // Implement DwEkKontaktProvider for Lpr3KontaktRegistry
    impl DwEkKontaktProvider for Lpr3KontaktRegistry {
        fn dw_ek_kontakt(&self) -> Option<String> {
            self.dw_ek_kontakt.clone()
        }
    }

    // Implement DwEkKontaktProvider for Lpr3DiagnoserRegistry
    impl DwEkKontaktProvider for Lpr3DiagnoserRegistry {
        fn dw_ek_kontakt(&self) -> Option<String> {
            self.dw_ek_kontakt.clone()
        }
    }

    // Implement DiagnosisMapper for Lpr3DiagnoserRegistry
    impl DiagnosisMapper for Lpr3DiagnoserRegistry {
        fn to_diagnosis(&self, pnr_lookup: &HashMap<String, String>) -> Option<Diagnosis> {
            // Get the DW_EK_KONTAKT
            let dw_ek_kontakt = self.dw_ek_kontakt.as_ref()?;

            // Look up the PNR from the DW_EK_KONTAKT
            let pnr = pnr_lookup.get(dw_ek_kontakt)?;

            // Get the diagnosis code
            let diagnosis_code = self.diagnosis_code.as_ref()?;

            // Determine diagnosis type
            let diagnosis_type = match self.diagnosis_type.as_deref() {
                Some("H") => DiagnosisType::Primary,     // Hoveddiagnose
                Some("B") => DiagnosisType::Secondary,   // Bidiagnose
                _ => DiagnosisType::Other,
            };

            // Create a new Diagnosis (without a date for now)
            let diagnosis = Diagnosis::new(
                pnr.clone(),
                diagnosis_code.clone(),
                diagnosis_type,
                None, // We don't have the date in the Diagnoser record
            );

            Some(diagnosis)
        }
    }

    // Create deserializers for both registry types
    let kontakt_deserializer = Lpr3KontaktRegistryDeserializer::new();
    let diagnoser_deserializer = Lpr3DiagnoserRegistryDeserializer::new();

    // Print deserializer info
    println!(
        "Created deserializer for {} registry",
        kontakt_deserializer.inner.registry_type()
    );
    println!(
        "Created deserializer for {} registry",
        diagnoser_deserializer.inner.registry_type()
    );

    // Path to the LPR3 Parquet files - adjust these to your actual file paths
    let kontakt_parquet_path =
        Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr3_kontakt/2020.parquet");
    let diagnoser_parquet_path =
        Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr3_diagnoser/2020.parquet");

    // Process LPR3 Kontakt first to build the DW_EK_KONTAKT to PNR mapping
    if kontakt_parquet_path.exists() {
        println!("\nProcessing LPR3 Kontakt data to build DW_EK_KONTAKT to PNR mapping...");

        // Load the Parquet file
        match read_parquet(kontakt_parquet_path) {
            Ok(batches) => {
                println!("Successfully loaded {} Kontakt record batches", batches.len());

                if let Some(batch) = batches.first() {
                    // Create a mapping from DW_EK_KONTAKT to PNR
                    let mut dw_ek_kontakt_to_pnr = DwEkKontaktToPnrMap::new();

                    // Deserialize the batch
                    match kontakt_deserializer.deserialize_batch(batch) {
                        Ok(kontakt_records) => {
                            // Build the DW_EK_KONTAKT to PNR mapping from registry records
                            for registry in &kontakt_records {
                                if let (Some(dw_ek_kontakt), Some(pnr)) =
                                    (registry.dw_ek_kontakt.clone(), Some(registry.pnr.clone()))
                                {
                                    dw_ek_kontakt_to_pnr.add_mapping(dw_ek_kontakt, pnr);
                                }
                            }

                            println!(
                                "Built mapping for {} DW_EK_KONTAKT IDs from Kontakt records",
                                dw_ek_kontakt_to_pnr.dw_ek_kontakt_to_pnr.len()
                            );

                            // Now process LPR3 Diagnoser
                            if diagnoser_parquet_path.exists() {
                                println!("\nProcessing LPR3 Diagnoser data...");

                                match read_parquet(diagnoser_parquet_path) {
                                    Ok(diagnoser_batches) => {
                                        println!(
                                            "Successfully loaded {} Diagnoser record batches",
                                            diagnoser_batches.len()
                                        );

                                        if let Some(diagnoser_batch) = diagnoser_batches.first() {
                                            match diagnoser_deserializer.deserialize_batch(diagnoser_batch) {
                                                Ok(diagnoser_records) => {
                                                    println!(
                                                        "Successfully deserialized {} Diagnoser records",
                                                        diagnoser_records.len()
                                                    );

                                                    // Count how many diagnoses have a valid DW_EK_KONTAKT that can be mapped to a PNR
                                                    let mut mapped_count = 0;

                                                    // Create diagnoses using the DiagnosisMapper
                                                    let mut diagnoses = Vec::new();
                                                    for diagnoser_record in &diagnoser_records {
                                                        if let Some(dw_ek_kontakt) =
                                                            &diagnoser_record.dw_ek_kontakt
                                                        {
                                                            if dw_ek_kontakt_to_pnr
                                                                .dw_ek_kontakt_to_pnr
                                                                .contains_key(dw_ek_kontakt)
                                                            {
                                                                mapped_count += 1;

                                                                // Create a diagnosis from the registry record
                                                                if let Some(diagnosis) = diagnoser_record
                                                                    .to_diagnosis(
                                                                        &dw_ek_kontakt_to_pnr
                                                                            .dw_ek_kontakt_to_pnr,
                                                                    )
                                                                {
                                                                    diagnoses.push(diagnosis);
                                                                }
                                                            }
                                                        }
                                                    }

                                                    println!(
                                                        "Found {} Diagnoser records with valid DW_EK_KONTAKT mappings",
                                                        mapped_count
                                                    );
                                                    println!(
                                                        "Created {} Diagnosis objects from Diagnoser records",
                                                        diagnoses.len()
                                                    );

                                                    // Display a few sample diagnoses
                                                    let limit = std::cmp::min(5, diagnoses.len());
                                                    if limit > 0 {
                                                        println!("\nSample diagnoses:");
                                                        for (i, diagnosis) in
                                                            diagnoses.iter().take(limit).enumerate()
                                                        {
                                                            println!(
                                                                "[{}] Patient: {}, Code: {}, Type: {:?}",
                                                                i + 1,
                                                                diagnosis.individual_pnr,
                                                                diagnosis.diagnosis_code,
                                                                diagnosis.diagnosis_type
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(e) => eprintln!(
                                                    "Error deserializing Diagnoser batch: {}",
                                                    e
                                                ),
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!("Error reading Diagnoser parquet: {}", e),
                                }
                            } else {
                                println!(
                                    "\nSkipping Diagnoser processing - file not found at: {}",
                                    diagnoser_parquet_path.display()
                                );
                            }
                        }
                        Err(e) => eprintln!("Error deserializing Kontakt batch: {}", e),
                    }
                }
            }
            Err(e) => eprintln!("Error reading Kontakt parquet: {}", e),
        }
    } else {
        println!(
            "\nSkipping Kontakt processing - file not found at: {}",
            kontakt_parquet_path.display()
        );
    }

    println!("\nLPR3 example completed!");
    Ok(())
}