//! Example demonstrating direct LPR registry-to-Diagnosis model integration
//!
//! This example shows how to use the direct model conversion traits to load data from 
//! LPR registries and automatically convert it to Diagnosis models.

use crate::registry::{LprDiagRegister, Lpr3DiagnoserRegister, ModelConversionExt, PnrLookupRegistry};
use crate::models::diagnosis::{Diagnosis, ScdResult};
use crate::error::Result;
use std::path::Path;
use std::collections::HashMap;

/// Example function showing how to load LPR2 data as Diagnosis models directly
pub fn load_diagnoses_example(base_path: &Path) -> Result<()> {
    // Create a map from record IDs to PNRs
    // In a real application, this would be loaded from LPR_ADM
    let mut pnr_lookup = HashMap::new();
    pnr_lookup.insert("12345".to_string(), "0101012222".to_string());
    pnr_lookup.insert("67890".to_string(), "0202023333".to_string());
    
    // Create an LPR2 registry with the PNR lookup
    let mut lpr_diag_registry = LprDiagRegister::new();
    lpr_diag_registry.set_pnr_lookup(pnr_lookup);
    
    // Load LPR2 DIAG data directly as Diagnosis models
    let diagnoses = lpr_diag_registry.load_as::<Diagnosis>(base_path, None)?;
    
    println!("Loaded {} diagnoses from LPR2 registry", diagnoses.len());
    
    // Process SCD results
    let scd_results = lpr_diag_registry.process_scd_results(&diagnoses);
    println!("Found {} individuals with SCD information", scd_results.len());
    
    // Use the first few diagnoses as examples
    if !diagnoses.is_empty() {
        println!("\nExample diagnoses:");
        for (i, diagnosis) in diagnoses.iter().take(5).enumerate() {
            println!("Diagnosis {}: PNR={}, Code={}, Type={:?}, Is SCD={}", 
                i + 1, 
                diagnosis.individual_pnr, 
                diagnosis.diagnosis_code,
                diagnosis.diagnosis_type,
                diagnosis.is_scd
            );
        }
    }
    
    Ok(())
}

/// Example function showing how to load LPR3 data as Diagnosis models directly
pub fn load_lpr3_diagnoses_example(base_path: &Path) -> Result<()> {
    // Create a map from kontakt IDs to PNRs
    // In a real application, this would be loaded from LPR3_KONTAKTER
    let mut pnr_lookup = HashMap::new();
    pnr_lookup.insert("K12345".to_string(), "0101012222".to_string());
    pnr_lookup.insert("K67890".to_string(), "0202023333".to_string());
    
    // Create an LPR3 registry with the PNR lookup
    let lpr3_registry = Lpr3DiagnoserRegister::with_pnr_lookup(pnr_lookup);
    
    // Load LPR3 DIAGNOSER data directly as Diagnosis models
    let diagnoses = lpr3_registry.load_as::<Diagnosis>(base_path, None)?;
    
    println!("Loaded {} diagnoses from LPR3 registry", diagnoses.len());
    
    // Process SCD results
    let scd_results = lpr3_registry.process_scd_results(&diagnoses);
    println!("Found {} individuals with SCD information", scd_results.len());
    
    Ok(())
}

/// Run the LPR model integration examples
pub fn run_lpr_model_integration_examples() -> Result<()> {
    println!("=== LPR Registry-Model Integration Example ===");
    println!("NOTE: This example requires LPR registry data");
    println!("Skipping actual data loading for demonstration purposes");
    
    // Here we would normally call:
    // load_diagnoses_example(Path::new("path/to/lpr2/diag/registry"))?;
    // load_lpr3_diagnoses_example(Path::new("path/to/lpr3/diagnoser/registry"))?;
    
    // For demonstration, show the approach
    println!("\nTo load diagnoses directly from LPR2 registry:");
    println!("let mut lpr_diag_registry = LprDiagRegister::new();");
    println!("lpr_diag_registry.set_pnr_lookup(pnr_lookup);");
    println!("let diagnoses = lpr_diag_registry.load_as::<Diagnosis>(base_path, None)?;");
    
    println!("\nTo load diagnoses directly from LPR3 registry:");
    println!("let lpr3_registry = Lpr3DiagnoserRegister::with_pnr_lookup(pnr_lookup);");
    println!("let diagnoses = lpr3_registry.load_as::<Diagnosis>(base_path, None)?;");
    
    println!("\nTo process SCD results:");
    println!("let scd_results = lpr_registry.process_scd_results(&diagnoses);");
    
    println!("\nExample SCD result usage:");
    let mut results = HashMap::new();
    
    // Example SCD result
    let mut result = ScdResult::new("0101012222".to_string());
    result.has_scd = true;
    result.max_severity = 3;
    result.scd_categories = vec![1, 4]; // Blood/neoplasm and Neurological
    results.insert(result.pnr.clone(), result);
    
    // Display results
    println!("\nSCD Analysis Results:");
    for (_pnr, result) in &results {
        println!("  - Individual has SCD: {}", result.has_scd);
        println!("  - Max severity: {}", result.max_severity);
        println!("  - Categories: {:?}", result.scd_categories);
        println!("  - Has congenital condition: {}", result.has_congenital);
    }
    
    Ok(())
}