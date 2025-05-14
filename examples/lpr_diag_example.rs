//! Example for LPR_DIAG registry using RECNUM as the primary identifier field.
//!
//! This example demonstrates how to use the LPR_DIAG registry with RECNUM
//! as the primary identifier field instead of PNR.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use par_reader::models::core::types::DiagnosisType;
use par_reader::models::health::{
    Diagnosis, DiagnosisMapper, PnrProvider, RecnumProvider, RecnumToPnrMap,
};
use par_reader::*;
use std::collections::HashMap;
use std::path::Path;

/// Simple wrapper around par_reader::read_parquet for a single file
fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    // Use the built-in utility function
    par_reader::read_parquet::<std::collections::hash_map::RandomState>(
        path, None, None, None, None,
    )
}

/// Read all Parquet files from a directory
fn read_parquet_dir(dir_path: &Path) -> Result<Vec<RecordBatch>> {
    use std::fs;

    // Check if directory exists
    if !dir_path.exists() || !dir_path.is_dir() {
        return Err(anyhow::anyhow!(
            "Directory not found or not a directory: {}",
            dir_path.display()
        ));
    }

    // Initialize empty result vector
    let mut all_batches = Vec::new();

    // Read directory contents
    println!(
        "Reading Parquet files from directory: {}",
        dir_path.display()
    );
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        // Skip non-Parquet files
        if path.is_file() && path.extension().map_or(false, |ext| ext == "parquet") {
            println!(
                "  Processing file: {}",
                path.file_name().unwrap_or_default().to_string_lossy()
            );

            // Read the Parquet file
            match read_parquet(&path) {
                Ok(mut batches) => {
                    all_batches.append(&mut batches);
                }
                Err(e) => {
                    println!("  Error reading file: {}", e);
                }
            }
        }
    }

    println!("  Total batches loaded: {}", all_batches.len());
    Ok(all_batches)
}

fn main() -> Result<()> {
    println!("LPR_DIAG Example");

    // Define LPR ADM Registry with PNR as the identifier
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "LPR_ADM", description = "LPR Administrative", id_field = "pnr")]
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

        #[field(name = "D_INDDTO")]
        admission_date: Option<NaiveDate>,
    }

    // Define LPR DIAG Registry using RECNUM as the identifier
    // Ensure our field names exactly match the Parquet column names
    #[derive(RegistryTrait, Debug)]
    #[registry(
        name = "LPR_DIAG",
        description = "LPR Diagnosis registry",
        id_field = "record_number"
    )]
    struct LprDiagRegistry {
        // Reference to ADM record - capitalization must match the Parquet schema
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

    // Paths to the LPR Parquet directories
    let adm_parquet_dir = Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_adm");
    let diag_parquet_dir = Path::new("/Users/tobiaskragholm/generated_data/parquet/lpr_diag");

    // Process LPR ADM first to build the RECNUM to PNR mapping
    if adm_parquet_dir.exists() {
        println!("\nProcessing LPR ADM data to build RECNUM to PNR mapping...");

        // Load all Parquet files from the directory
        match read_parquet_dir(adm_parquet_dir) {
            Ok(adm_batches) => {
                println!(
                    "Successfully loaded {} ADM record batches",
                    adm_batches.len()
                );

                // Create a mapping from RECNUM to PNR
                let mut recnum_to_pnr = RecnumToPnrMap::new();

                // Process each batch
                for batch in &adm_batches {
                    // Deserialize the batch
                    match adm_deserializer.deserialize_batch(batch) {
                        Ok(adm_records) => {
                            // Build the RECNUM to PNR mapping from registry records
                            for registry in &adm_records {
                                if let (Some(recnum), Some(pnr)) =
                                    (registry.record_number.clone(), Some(registry.pnr.clone()))
                                {
                                    recnum_to_pnr.add_mapping(recnum, pnr);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error deserializing ADM batch: {}", e);
                            continue; // Skip this batch and continue with the next
                        }
                    }
                }

                println!(
                    "Built mapping for {} RECNUMs from ADM records",
                    recnum_to_pnr.recnum_to_pnr.len()
                );

                // Now process LPR DIAG
                if diag_parquet_dir.exists() {
                    println!("\nProcessing LPR DIAG data...");

                    match read_parquet_dir(diag_parquet_dir) {
                        Ok(diag_batches) => {
                            println!(
                                "Successfully loaded {} DIAG record batches",
                                diag_batches.len()
                            );

                            // Print raw schema of the first batch to see what fields are available
                            if !diag_batches.is_empty() {
                                let first_batch = &diag_batches[0];
                                println!("\nDEBUG: DIAG batch schema:");
                                println!("{:#?}", first_batch.schema());

                                // Print a few rows in raw format to inspect column names
                                println!("\nDEBUG: DIAG column names:");
                                for (i, field) in first_batch.schema().fields().iter().enumerate() {
                                    println!("  {}: {}", i, field.name());
                                }

                                // Examine the actual data in the first batch
                                if first_batch.num_rows() > 0 {
                                    println!("\nDEBUG: Examining first few DIAG records:");
                                    let recnum_col_index = first_batch
                                        .schema()
                                        .fields()
                                        .iter()
                                        .position(|f| f.name() == "RECNUM")
                                        .unwrap_or(0);

                                    let diag_col_index = first_batch
                                        .schema()
                                        .fields()
                                        .iter()
                                        .position(|f| f.name() == "C_DIAG")
                                        .unwrap_or(0);

                                    let diagtype_col_index = first_batch
                                        .schema()
                                        .fields()
                                        .iter()
                                        .position(|f| f.name() == "C_DIAGTYPE")
                                        .unwrap_or(0);

                                    for i in 0..std::cmp::min(5, first_batch.num_rows()) {
                                        // Access the columns directly
                                        let recnum_col = first_batch.column(recnum_col_index);
                                        let diag_col = first_batch.column(diag_col_index);
                                        let diagtype_col = first_batch.column(diagtype_col_index);

                                        // Print row data
                                        println!("Row {}:", i);
                                        println!(
                                            "  RECNUM: {}",
                                            if recnum_col.is_null(i) {
                                                "NULL"
                                            } else {
                                                recnum_col
                                                    .as_any()
                                                    .downcast_ref::<arrow::array::StringArray>()
                                                    .unwrap()
                                                    .value(i)
                                            }
                                        );
                                        println!(
                                            "  C_DIAG: {}",
                                            if diag_col.is_null(i) {
                                                "NULL"
                                            } else {
                                                diag_col
                                                    .as_any()
                                                    .downcast_ref::<arrow::array::StringArray>()
                                                    .unwrap()
                                                    .value(i)
                                            }
                                        );
                                        println!(
                                            "  C_DIAGTYPE: {}",
                                            if diagtype_col.is_null(i) {
                                                "NULL"
                                            } else {
                                                diagtype_col
                                                    .as_any()
                                                    .downcast_ref::<arrow::array::StringArray>()
                                                    .unwrap()
                                                    .value(i)
                                            }
                                        );
                                    }
                                }
                            }

                            // Count the total rows in all batches
                            let total_rows = diag_batches
                                .iter()
                                .map(|batch| batch.num_rows())
                                .sum::<usize>();
                            println!("\nDEBUG: Total rows in all DIAG batches: {}", total_rows);

                            // Initialize counters
                            let mut total_diag_records = 0;
                            let mut mapped_count = 0;
                            let mut diagnoses = Vec::new();

                            // Process each DIAG batch
                            for diag_batch in &diag_batches {
                                // Instead of using the high-level deserialize_batch, let's try to extract records manually
                                // to understand what's happening

                                // Get the column indices for the fields we care about
                                let recnum_idx = diag_batch
                                    .schema()
                                    .fields()
                                    .iter()
                                    .position(|f| f.name() == "RECNUM")
                                    .unwrap_or(usize::MAX);

                                let diag_idx = diag_batch
                                    .schema()
                                    .fields()
                                    .iter()
                                    .position(|f| f.name() == "C_DIAG")
                                    .unwrap_or(usize::MAX);

                                let diagtype_idx = diag_batch
                                    .schema()
                                    .fields()
                                    .iter()
                                    .position(|f| f.name() == "C_DIAGTYPE")
                                    .unwrap_or(usize::MAX);

                                // Extract values directly from the Arrow arrays
                                let mut extracted_records = Vec::new();

                                if recnum_idx != usize::MAX
                                    && diag_idx != usize::MAX
                                    && diagtype_idx != usize::MAX
                                {
                                    let recnum_col = diag_batch
                                        .column(recnum_idx)
                                        .as_any()
                                        .downcast_ref::<arrow::array::StringArray>()
                                        .unwrap();
                                    let diag_col = diag_batch
                                        .column(diag_idx)
                                        .as_any()
                                        .downcast_ref::<arrow::array::StringArray>()
                                        .unwrap();
                                    let diagtype_col = diag_batch
                                        .column(diagtype_idx)
                                        .as_any()
                                        .downcast_ref::<arrow::array::StringArray>()
                                        .unwrap();

                                    for i in 0..diag_batch.num_rows() {
                                        if !recnum_col.is_null(i) && !diag_col.is_null(i) {
                                            let recnum = recnum_col.value(i).to_string();
                                            let diag = diag_col.value(i).to_string();
                                            let diag_type = if diagtype_col.is_null(i) {
                                                None
                                            } else {
                                                Some(diagtype_col.value(i).to_string())
                                            };

                                            // Create a LprDiagRegistry struct manually
                                            let diag_record = LprDiagRegistry {
                                                record_number: Some(recnum),
                                                diagnosis_code: Some(diag),
                                                diagnosis_type: diag_type,
                                            };

                                            // Check if this RECNUM has a PNR mapping
                                            if let Some(recnum_str) = &diag_record.record_number {
                                                if recnum_to_pnr
                                                    .recnum_to_pnr
                                                    .contains_key(recnum_str)
                                                {
                                                    // Create a diagnosis from the registry record
                                                    if let Some(diagnosis) = diag_record
                                                        .to_diagnosis(&recnum_to_pnr.recnum_to_pnr)
                                                    {
                                                        diagnoses.push(diagnosis);
                                                        mapped_count += 1;
                                                    }
                                                }
                                            }

                                            extracted_records.push(diag_record);
                                        }
                                    }

                                    println!(
                                        "\nDEBUG: Manually extracted {} DIAG records from batch",
                                        extracted_records.len()
                                    );

                                    // Check first few records to see if they have RECNUMs
                                    if !extracted_records.is_empty() {
                                        println!("First few extracted records:");
                                        for (i, record) in
                                            extracted_records.iter().take(5).enumerate()
                                        {
                                            println!(
                                                "[{}] RECNUM: '{:?}', Diag: '{:?}', Type: '{:?}'",
                                                i + 1,
                                                record.record_number,
                                                record.diagnosis_code,
                                                record.diagnosis_type
                                            );

                                            // Check if the RECNUM exists in our mapping
                                            if let Some(recnum) = &record.record_number {
                                                let has_mapping = recnum_to_pnr
                                                    .recnum_to_pnr
                                                    .contains_key(recnum);
                                                println!(
                                                    "  RECNUM '{}' exists in mapping: {}",
                                                    recnum, has_mapping
                                                );
                                            }
                                        }
                                    }
                                }

                                total_diag_records += extracted_records.len();

                                // Skip the standard deserialization - it's not working correctly
                                match diag_deserializer.deserialize_batch(diag_batch) {
                                    Ok(diag_records) => {
                                        println!(
                                            "Standard deserializer returned {} records (vs {} extracted manually)",
                                            diag_records.len(),
                                            extracted_records.len()
                                        );
                                        // Print some debug info for the first batch
                                        if total_diag_records == 0 && !diag_records.is_empty() {
                                            println!(
                                                "\nDEBUG: First DIAG batch contains {} records",
                                                diag_records.len()
                                            );
                                            let sample_count = std::cmp::min(5, diag_records.len());

                                            println!("Sample DIAG records from first batch:");
                                            for (i, record) in
                                                diag_records.iter().take(sample_count).enumerate()
                                            {
                                                println!(
                                                    "[{}] RECNUM: '{:?}', Diagnosis: '{:?}', Type: '{:?}'",
                                                    i + 1,
                                                    record.record_number,
                                                    record.diagnosis_code,
                                                    record.diagnosis_type
                                                );
                                            }

                                            // Check if these RECNUM values exist in the mapping
                                            for record in diag_records.iter().take(sample_count) {
                                                if let Some(recnum) = &record.record_number {
                                                    let has_mapping = recnum_to_pnr
                                                        .recnum_to_pnr
                                                        .contains_key(recnum);
                                                    println!(
                                                        "RECNUM '{}' exists in mapping: {}",
                                                        recnum, has_mapping
                                                    );
                                                }
                                            }
                                        }

                                        total_diag_records += diag_records.len();

                                        // Create diagnoses using the DiagnosisMapper
                                        for diag_record in &diag_records {
                                            if let Some(recnum) = &diag_record.record_number {
                                                if recnum_to_pnr.recnum_to_pnr.contains_key(recnum)
                                                {
                                                    mapped_count += 1;

                                                    // Create a diagnosis from the registry record
                                                    if let Some(diagnosis) = diag_record
                                                        .to_diagnosis(&recnum_to_pnr.recnum_to_pnr)
                                                    {
                                                        diagnoses.push(diagnosis);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Error deserializing DIAG batch: {}", e);
                                        continue; // Skip this batch and continue with the next
                                    }
                                }
                            }

                            println!(
                                "Successfully deserialized {} DIAG records",
                                total_diag_records
                            );
                            println!(
                                "Found {} DIAG records with valid RECNUM mappings",
                                mapped_count
                            );
                            println!(
                                "Created {} Diagnosis objects from DIAG records",
                                diagnoses.len()
                            );

                            // Display a few sample diagnoses
                            let limit = std::cmp::min(5, diagnoses.len());
                            if limit > 0 {
                                println!("\nSample diagnoses:");
                                for (i, diagnosis) in diagnoses.iter().take(limit).enumerate() {
                                    println!(
                                        "[{}] Patient: {}, Code: {}, Type: {:?}",
                                        i + 1,
                                        diagnosis.individual_pnr,
                                        diagnosis.diagnosis_code,
                                        diagnosis.diagnosis_type
                                    );
                                }
                            } else {
                                println!(
                                    "\nNo diagnoses were created. This may be because either:"
                                );
                                println!("1. No valid RECNUM to PNR mappings were found");
                                println!("2. The diagnosis records don't have valid RECNUM values");
                                println!("3. The RECNUM values in DIAG don't match any in ADM");

                                // DEBUG: Print a few record_number values from both ADM and DIAG
                                if !recnum_to_pnr.recnum_to_pnr.is_empty() {
                                    println!("\nSample RECNUM values from ADM records:");
                                    for (i, (recnum, pnr)) in
                                        recnum_to_pnr.recnum_to_pnr.iter().take(3).enumerate()
                                    {
                                        println!(
                                            "[{}] RECNUM: '{}', PNR: '{}'",
                                            i + 1,
                                            recnum,
                                            pnr
                                        );
                                    }
                                }

                                // Find and print a few DIAG records to see their RECNUM format
                                let mut diag_samples_found = false;
                                for diag_batch in &diag_batches {
                                    match diag_deserializer.deserialize_batch(diag_batch) {
                                        Ok(diag_records) => {
                                            if !diag_records.is_empty() {
                                                println!(
                                                    "\nSample DIAG records with their RECNUM values:"
                                                );
                                                for (i, record) in
                                                    diag_records.iter().take(5).enumerate()
                                                {
                                                    println!(
                                                        "[{}] RECNUM: '{:?}', Diagnosis: '{:?}', Type: '{:?}'",
                                                        i + 1,
                                                        record.record_number,
                                                        record.diagnosis_code,
                                                        record.diagnosis_type
                                                    );
                                                }
                                                diag_samples_found = true;
                                                break;
                                            }
                                        }
                                        Err(_) => continue,
                                    }
                                }

                                if !diag_samples_found {
                                    println!("\nNo DIAG records found to display as samples.");
                                }
                            }
                        }
                        Err(e) => eprintln!("Error reading DIAG parquet directory: {}", e),
                    }
                } else {
                    println!(
                        "\nSkipping DIAG processing - directory not found at: {}",
                        diag_parquet_dir.display()
                    );
                }
            }
            Err(e) => eprintln!("Error reading ADM parquet directory: {}", e),
        }
    } else {
        println!(
            "\nSkipping ADM processing - directory not found at: {}",
            adm_parquet_dir.display()
        );
    }

    println!("\nLPR_DIAG example completed!");
    Ok(())
}
