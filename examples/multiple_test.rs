//! Test for different ID field types in registry deserializers
//!
//! This example demonstrates the new support for different ID field types
//! in registry deserializers, including:
//! - PNR (Personal Identification Number) - Default
//! - RECNUM (Record Number) - Used in `LPR_DIAG`

// Remove unused import
use chrono::NaiveDate;
use par_reader::{ModelCollection, RegistryTrait, error, models, registry, schema};
use std::collections::HashMap;
use std::path::Path;

/// Registry using PNR as ID field (default)
#[derive(RegistryTrait, Debug)]
#[registry(name = "BEF", description = "Population registry", id_field = "pnr")]
pub struct BefRegistry {
    #[field(name = "PNR")]
    pub pnr: String,

    #[field(name = "FOED_DAG")]
    pub birth_date: Option<NaiveDate>,

    #[field(name = "KOEN")]
    pub gender: Option<String>,
}

/// Registry using RECNUM as ID field
#[derive(RegistryTrait, Debug)]
#[registry(
    name = "LPR_ADM",
    description = "LPR Admission registry",
    id_field = "pnr"
)]
pub struct LprAdmRegistry {
    #[field(name = "PNR")]
    pub pnr: String,

    #[field(name = "RECNUM")]
    pub record_number: Option<String>,

    #[field(name = "D_INDDTO")]
    pub admission_date: Option<NaiveDate>,

    #[field(name = "SYGEHUS_REGION")]
    pub hospital_region: Option<String>,
}

/// Registry using RECNUM as ID field
#[derive(RegistryTrait, Debug)]
#[registry(
    name = "LPR_DIAG",
    description = "LPR Diagnosis registry",
    id_field = "record_number"
)]
pub struct LprDiagRegistry {
    #[field(name = "RECNUM")]
    pub record_number: Option<String>,

    #[field(name = "C_DIAG")]
    pub diagnosis_code: Option<String>,

    #[field(name = "C_DIAGTYPE")]
    pub diagnosis_type: Option<String>,
}

/// Create a mapping from RECNUM to PNR using ADM records
fn create_recnum_to_pnr_mapping(adm_records: &[LprAdmRegistry]) -> HashMap<String, String> {
    let mut mapping = HashMap::new();

    for record in adm_records {
        if let Some(recnum) = &record.record_number {
            // Only map if both PNR and RECNUM are valid
            if !record.pnr.is_empty() && !recnum.is_empty() {
                mapping.insert(recnum.clone(), record.pnr.clone());
            }
        }
    }

    println!(
        "Created mapping with {} RECNUM -> PNR pairs\n",
        mapping.len()
    );
    mapping
}

/// Run the test for different ID field types
pub fn run_id_field_test() {
    println!("Testing different ID field types in registry deserializers");

    // Path to a BEF Parquet file (uses PNR as ID)
    let bef_path = Path::new("/Users/tobiaskragholm/generated_data/parquet/bef/202209.parquet");
    println!("Loading BEF data from: {bef_path:?}");

    // Path to LPR ADM Parquet file (uses PNR as ID but has RECNUM for joining)
    let lpr_adm_path =
        Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_adm/2000.parquet");
    println!("Loading LPR ADM data from: {lpr_adm_path:?}");

    // Path to LPR DIAG Parquet file (uses RECNUM as ID)
    let lpr_diag_path =
        Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_diag/2000.parquet");
    println!("Loading LPR DIAG data from: {lpr_diag_path:?}");

    // Create deserializers for each registry
    let bef_deserializer = BefRegistryDeserializer::new();
    let adm_deserializer = LprAdmRegistryDeserializer::new();
    let diag_deserializer = LprDiagRegistryDeserializer::new();

    // Print the ID field type for each deserializer
    println!(
        "BEF deserializer ID field type: {}",
        bef_deserializer.inner.id_field_type()
    );
    println!(
        "LPR ADM deserializer ID field type: {}",
        adm_deserializer.inner.id_field_type()
    );
    println!(
        "LPR DIAG deserializer ID field type: {}",
        diag_deserializer.inner.id_field_type()
    );

    // Load the BEF data
    match par_reader::loader::read_parquet(bef_path, None, None) {
        Ok(bef_batches) if !bef_batches.is_empty() => {
            println!(
                "Successfully loaded BEF batch with {} rows",
                bef_batches[0].num_rows()
            );

            // Deserialize the BEF records
            let bef_records: Vec<BefRegistry> = bef_deserializer
                .deserialize_batch(&bef_batches[0])
                .unwrap_or_default();

            println!("Deserialized {} BEF records", bef_records.len());

            // Print the first few records
            for (i, record) in bef_records.iter().take(5).enumerate() {
                println!(
                    "[{}] PNR: {}, Birth Date: {:?}, Gender: {:?}",
                    i + 1,
                    record.pnr,
                    record.birth_date,
                    record.gender
                );
            }
        }
        Ok(_) => {
            println!("No BEF batches found");
        }
        Err(err) => {
            eprintln!("Error loading BEF data: {err}");
        }
    }

    // Load the LPR ADM data
    let adm_records = match par_reader::loader::read_parquet(lpr_adm_path, None, None) {
        Ok(adm_batches) if !adm_batches.is_empty() => {
            println!(
                "Successfully loaded LPR ADM batch with {} rows",
                adm_batches[0].num_rows()
            );

            // Deserialize the ADM records
            let adm_records: Vec<LprAdmRegistry> = adm_deserializer
                .deserialize_batch(&adm_batches[0])
                .unwrap_or_default();

            println!("Deserialized {} LPR ADM records", adm_records.len());

            // Print the first few records
            for (i, record) in adm_records.iter().take(5).enumerate() {
                println!(
                    "[{}] PNR: {}, RECNUM: {:?}, Admission Date: {:?}",
                    i + 1,
                    record.pnr,
                    record.record_number,
                    record.admission_date
                );
            }

            adm_records
        }
        Ok(_) => {
            println!("No LPR ADM batches found");
            Vec::new()
        }
        Err(err) => {
            eprintln!("Error loading LPR ADM data: {err}");
            Vec::new()
        }
    };

    // Create a mapping from RECNUM to PNR using the ADM records
    let recnum_to_pnr = create_recnum_to_pnr_mapping(&adm_records);

    // Load the LPR DIAG data
    match par_reader::loader::read_parquet(lpr_diag_path, None, None) {
        Ok(diag_batches) if !diag_batches.is_empty() => {
            println!(
                "Successfully loaded LPR DIAG batch with {} rows\n",
                diag_batches[0].num_rows()
            );

            println!("\nDeserializing LPR_DIAG records using trait deserializer...");

            // Use the trait deserializer to convert Arrow RecordBatch to LprDiagRegistry objects
            let diag_records: Vec<LprDiagRegistry> = diag_deserializer
                .deserialize_batch(&diag_batches[0])
                .unwrap_or_default();

            println!("Deserialized {} LPR DIAG records", diag_records.len());

            // Print first few records
            for (i, record) in diag_records.iter().take(5).enumerate() {
                println!("Record {}: {:?}", i + 1, record);
            }

            println!("Deserialized {} LPR DIAG records", diag_records.len());

            // Fallback debugging if no records were found (should not happen with our fixes)
            if diag_records.is_empty() {
                println!(
                    "\nNo LPR_DIAG records were found. This indicates an issue with the deserialization."
                );
            }

            // Count matched records that have a corresponding PNR from the mapping
            let matched_count = diag_records
                .iter()
                .filter(|r| {
                    r.record_number
                        .as_ref()
                        .and_then(|recnum| recnum_to_pnr.get(recnum))
                        .is_some()
                })
                .count();

            // Print examples of LPR_DIAG records with their associated PNRs
            println!("\nExamples of DIAG records with matched PNRs:");
            let matched_records = diag_records
                .iter()
                .filter_map(|r| {
                    r.record_number
                        .as_ref()
                        .and_then(|recnum| recnum_to_pnr.get(recnum))
                        .map(|pnr| (r, pnr))
                })
                .take(5)
                .collect::<Vec<_>>();

            for (i, (record, pnr)) in matched_records.iter().enumerate() {
                println!(
                    "Record {}: RECNUM={:?}, Diagnosis={:?}, Type={:?}, PNR={}",
                    i + 1,
                    record.record_number,
                    record.diagnosis_code,
                    record.diagnosis_type,
                    pnr
                );
            }

            println!(
                "Found PNR matches for {}/{} DIAG records",
                matched_count,
                diag_records.len()
            );
        }
        Ok(_) => {
            println!("No LPR DIAG batches found");
        }
        Err(err) => {
            eprintln!("Error loading LPR DIAG data: {err}");
        }
    }
}

pub fn main() {
    run_id_field_test();
}
