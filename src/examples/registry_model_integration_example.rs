//! Example demonstrating direct registry-to-model integration
//!
//! This example shows how to use the direct model conversion traits to load data from 
//! a registry and automatically convert it to domain models without manual adapter code.

use crate::registry::{BefRegister, ModelConversionExt, RegisterLoader};
use crate::models::{Individual, Family};
use crate::error::Result;
use std::path::Path;

/// Example function showing how to load BEF data as Individual models directly
pub fn load_individuals_example(base_path: &Path) -> Result<()> {
    // Create a BEF registry loader
    let bef_registry = BefRegister::new();
    
    // Load BEF data directly as Individual models
    let individuals = bef_registry.load_as::<Individual>(base_path, None)?;
    
    println!("Loaded {} individuals from BEF registry", individuals.len());
    
    // Use the first few individuals as examples
    if !individuals.is_empty() {
        println!("\nExample individuals:");
        for (i, individual) in individuals.iter().take(5).enumerate() {
            println!("Individual {}: PNR={}, Gender={:?}", 
                i + 1, 
                individual.pnr, 
                individual.gender
            );
        }
    }
    
    Ok(())
}

/// Example function showing how to load BEF data as Family models directly
pub fn load_families_example(base_path: &Path) -> Result<()> {
    // Create a BEF registry loader
    let bef_registry = BefRegister::new();
    
    // Load BEF data directly as Family models
    let families = bef_registry.load_as::<Family>(base_path, None)?;
    
    println!("Loaded {} families from BEF registry", families.len());
    
    // Use the first few families as examples
    if !families.is_empty() {
        println!("\nExample families:");
        for (i, family) in families.iter().take(5).enumerate() {
            println!("Family {}: ID={}, Type={:?}", 
                i + 1, 
                family.id, 
                family.family_type
            );
        }
    }
    
    Ok(())
}

/// Run the examples
pub fn run_registry_model_integration_examples() -> Result<()> {
    println!("=== Registry-Model Integration Example ===");
    println!("NOTE: This example requires BEF registry data");
    println!("Skipping actual data loading for demonstration purposes");
    
    // Here we would normally call:
    // load_individuals_example(Path::new("path/to/bef/registry"))?;
    // load_families_example(Path::new("path/to/bef/registry"))?;
    
    // For demonstration, show the approach
    println!("\nTo load individuals directly from BEF registry:");
    println!("let individuals = bef_registry.load_as::<Individual>(base_path, None)?;");
    
    println!("\nTo load families directly from BEF registry:");
    println!("let families = bef_registry.load_as::<Family>(base_path, None)?;");
    
    println!("\nTo load with a PNR filter:");
    println!("let pnr_filter = HashSet::from([\"1234567890\".to_string()]);");
    println!("let individuals = bef_registry.load_as::<Individual>(base_path, Some(&pnr_filter))?;");
    
    println!("\nAsync loading:");
    println!("let individuals = bef_registry.load_as_async::<Individual>(base_path, None).await?;");
    
    Ok(())
}