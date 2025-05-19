//! Example of integrating BEF, AKM, and IND data
//!
//! This example demonstrates how to load data from BEF, AKM, and IND registries
//! using direct deserialization through the factory methods.

use log::info;
use par_reader::models::core::Individual;
use par_reader::registry::direct_registry_loader::DirectRegistryLoader;
use par_reader::registry::factory::registry_from_name;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup basic logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("Starting BEF-AKM-IND data integration example using direct deserialization");

    // Path to the data directories
    let base_dir = Path::new("/home/tkragholm/generated_data/parquet");
    let bef_path = base_dir.join("bef");
    let akm_path = base_dir.join("akm");
    let ind_path = base_dir.join("ind");

    // Step 1: Load BEF individuals first
    info!("Step 1: Loading BEF individuals using direct deserialization");
    let start = Instant::now();
    let bef_registry = registry_from_name("bef")?;
    let bef_batches = bef_registry.load(&bef_path, None)?;
    info!(
        "Loaded {} BEF batches in {:?}",
        bef_batches.len(),
        start.elapsed()
    );

    // Step 2: Deserialize BEF records to create a base set of individuals
    info!("Step 2: Deserializing BEF records directly");
    let start = Instant::now();
    let mut individuals_by_pnr = HashMap::new();

    // Create a direct registry loader for BEF
    let direct_loader = DirectRegistryLoader::new("BEF");

    for batch in &bef_batches {
        // Use the direct_loader to deserialize batches to Individual structs
        let individuals = direct_loader.deserialize_batch(batch)?;

        // Index individuals by PNR for later merging
        for individual in individuals {
            individuals_by_pnr.insert(individual.pnr.clone(), individual);
        }
    }

    info!(
        "Deserialized {} individuals from BEF in {:?}",
        individuals_by_pnr.len(),
        start.elapsed()
    );

    // Step 3: Load AKM data
    info!("Step 3: Loading AKM data using direct deserialization");
    let start = Instant::now();

    // Only load AKM data for individuals we have from BEF
    let pnr_filter: HashSet<String> = individuals_by_pnr.keys().cloned().collect();

    // Create a direct registry loader for AKM
    let akm_registry = registry_from_name("akm")?;
    let akm_batches = akm_registry.load(&akm_path, Some(&pnr_filter))?;

    info!(
        "Loaded {} AKM batches in {:?}",
        akm_batches.len(),
        start.elapsed()
    );

    // Step 4: Enhance individuals with AKM data
    info!("Step 4: Enhancing individuals with AKM data using direct deserialization");
    let start = Instant::now();
    let mut akm_match_count = 0;

    // Create a direct registry loader for AKM
    let direct_akm_loader = DirectRegistryLoader::new("AKM");

    for batch in &akm_batches {
        // Use the direct_loader to deserialize batches to Individual structs
        let akm_individuals = direct_akm_loader.deserialize_batch(batch)?;

        for akm_individual in akm_individuals {
            if let Some(individual) = individuals_by_pnr.get_mut(&akm_individual.pnr) {
                // Merge the AKM data into the existing individual
                individual.merge_fields(&akm_individual);
                akm_match_count += 1;
            }
        }
    }

    info!(
        "Enhanced {} individuals with AKM data in {:?}",
        akm_match_count,
        start.elapsed()
    );

    // Step 5: Load IND data
    info!("Step 5: Loading IND data using direct deserialization");
    let start = Instant::now();

    // Create a direct registry loader for IND
    let ind_registry = registry_from_name("ind")?;

    // Only load IND data for individuals we have from BEF
    let ind_batches = ind_registry.load(&ind_path, Some(&pnr_filter))?;

    info!(
        "Loaded {} IND batches in {:?}",
        ind_batches.len(),
        start.elapsed()
    );

    // Step 6: Enhance individuals with IND data
    info!("Step 6: Enhancing individuals with IND data using direct deserialization");
    let start = Instant::now();
    let mut ind_match_count = 0;

    // Create a direct registry loader for IND
    let direct_ind_loader = DirectRegistryLoader::new("IND");

    for batch in &ind_batches {
        // Use the direct_loader to deserialize batches to Individual structs
        let ind_individuals = direct_ind_loader.deserialize_batch(batch)?;

        for ind_individual in ind_individuals {
            if let Some(individual) = individuals_by_pnr.get_mut(&ind_individual.pnr) {
                // Merge the IND data into the existing individual
                individual.merge_fields(&ind_individual);
                ind_match_count += 1;
            }
        }
    }

    info!(
        "Enhanced {} individuals with IND data in {:?}",
        ind_match_count,
        start.elapsed()
    );

    // // Step 7: Alternative approach - load data using the load_multiple_registries function
    // info!("Step 7: Demonstrating load_multiple_registries function");
    // let start = Instant::now();

    // // Define the registries and paths
    // let registry_paths = [
    //     ("bef", bef_path.as_path()),
    //     ("akm", akm_path.as_path()),
    //     ("ind", ind_path.as_path()),
    // ];

    // // Load all registries at once
    // let all_batches = load_multiple_registries(&registry_paths, Some(&pnr_filter))?;

    // info!(
    //     "Loaded {} total batches from multiple registries in {:?}",
    //     all_batches.len(),
    //     start.elapsed()
    // );

    // Print some statistics about the enriched data
    let individuals: Vec<&Individual> = individuals_by_pnr.values().collect();
    print_enrichment_statistics(&individuals);

    info!("BEF-AKM-IND integration example completed successfully");
    Ok(())
}

/// Print statistics about the enriched individuals
fn print_enrichment_statistics(individuals: &[&Individual]) {
    println!("\n--- ENRICHMENT STATISTICS ---");
    println!("Total individuals: {}", individuals.len());

    // Count individuals with socioeconomic status from AKM
    let with_socioeconomic = individuals
        .iter()
        .filter(|i| i.socioeconomic_status.is_some())
        .count();

    println!(
        "Individuals with socioeconomic status: {} ({:.1}%)",
        with_socioeconomic,
        (with_socioeconomic as f64 / individuals.len() as f64) * 100.0
    );

    // Count individuals with income data from IND
    let with_income = individuals
        .iter()
        .filter(|i| i.annual_income.is_some())
        .count();

    println!(
        "Individuals with income data: {} ({:.1}%)",
        with_income,
        (with_income as f64 / individuals.len() as f64) * 100.0
    );

    // Count individuals with both AKM and IND data
    let with_both = individuals
        .iter()
        .filter(|i| i.socioeconomic_status.is_some() && i.annual_income.is_some())
        .count();

    println!(
        "Individuals with both socioeconomic status and income data: {} ({:.1}%)",
        with_both,
        (with_both as f64 / individuals.len() as f64) * 100.0
    );

    // Print sample of enriched individuals (first 5)
    println!("\n--- SAMPLE ENRICHED INDIVIDUALS ---");
    for (i, individual) in individuals.iter().take(5).enumerate() {
        println!(
            "Individual {}: PNR={}, Gender={:?}, Birth Date={:?}",
            i + 1,
            individual.pnr,
            individual.gender,
            individual.birth_date
        );
        println!(
            "  Socioeconomic Status: {:?}",
            individual.socioeconomic_status
        );
        println!("  Annual Income: {:?}", individual.annual_income);
        println!("  Employment Income: {:?}", individual.employment_income);
        println!();
    }
}
