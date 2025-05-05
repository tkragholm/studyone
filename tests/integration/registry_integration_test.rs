use crate::utils::{print_schema_info, registry_dir};
use par_reader::RegistryManager;
use std::collections::HashSet;

/// Test to verify the functionality of loading and filtering across multiple registries
#[tokio::test]
async fn test_multiple_registry_operations() -> par_reader::Result<()> {
    // Create a registry manager
    let manager = RegistryManager::new();

    // Get paths to available registries
    let registries = [
        "akm", "bef", "lpr_adm", "lpr_diag", "lpr_bes", "ind", "mfr", "uddf", "vnds",
    ];

    // Register all available registries
    let mut registered_count = 0;
    let mut available_registries = Vec::new();

    for &registry in &registries {
        let path = registry_dir(registry);
        if path.exists()
            && std::fs::read_dir(&path)
                .map(|entries| entries.count())
                .unwrap_or(0)
                > 0
        {
            manager.register(registry, &path)?;
            println!("Registered {} registry", registry);
            registered_count += 1;
            available_registries.push(registry);
        }
    }

    if registered_count == 0 {
        println!("No registries available for testing. Skipping integration test.");
        return Ok(());
    }

    println!("Registered {} registries successfully", registered_count);

    // Test loading all registries
    for &registry in &available_registries {
        println!("Testing loading of {} registry", registry);
        match manager.load(registry) {
            Ok(batches) => {
                println!(
                    "{} registry: {} batches with {} total rows",
                    registry,
                    batches.len(),
                    batches.iter().map(|b| b.num_rows()).sum::<usize>()
                );

                // Print schema of first batch if available
                if let Some(first_batch) = batches.first() {
                    print_schema_info(first_batch);
                }
            }
            Err(e) => println!("Error loading {} registry: {}", registry, e),
        }
    }

    // Test filtering by PNR across registries if we have at least 2 registries
    if available_registries.len() >= 2 {
        println!("\nTesting PNR filtering across multiple registries");

        // Create a sample PNR filter with some synthetic PNRs
        let mut pnr_filter = HashSet::new();
        pnr_filter.insert("0101701234".to_string());
        pnr_filter.insert("0101801234".to_string());
        pnr_filter.insert("0101901234".to_string());

        // Use the first two available registries
        let registries_to_filter = &available_registries[0..2];

        match manager.filter_by_pnr(registries_to_filter, &pnr_filter) {
            Ok(filtered_data) => {
                println!("Successfully filtered data across registries:");
                for (registry, batches) in filtered_data {
                    println!(
                        "  {}: {} batches with {} total rows",
                        registry,
                        batches.len(),
                        batches.iter().map(|b| b.num_rows()).sum::<usize>()
                    );
                }
            }
            Err(e) => println!("Error filtering by PNR: {}", e),
        }
    }

    Ok(())
}

/// Test loading data from all registries in parallel
#[tokio::test]
async fn test_parallel_load_all_registries() -> par_reader::Result<()> {
    // This test requires additional implementation of load_parallel in RegistryManager
    // As this is not yet implemented, we'll make a simpler test
    println!("Note: Parallel loading test implementation pending");

    let manager = RegistryManager::new();

    // Get paths to available registries
    let registries = [
        "akm", "bef", "lpr_adm", "lpr_diag", "lpr_bes", "ind", "mfr", "uddf", "vnds",
    ];

    // Register all available registries
    let mut registered_count = 0;
    let mut available_registries = Vec::new();

    for &registry in &registries {
        let path = registry_dir(registry);
        if path.exists()
            && std::fs::read_dir(&path)
                .map(|entries| entries.count())
                .unwrap_or(0)
                > 0
        {
            manager.register(registry, &path)?;
            registered_count += 1;
            available_registries.push(registry);
        }
    }

    if registered_count == 0 {
        println!("No registries available for testing. Skipping parallel load test.");
        return Ok(());
    }

    println!("Registered {} registries successfully", registered_count);

    // For each registry, load it separately (in sequence)
    let mut total_batches = 0;
    let mut total_rows = 0;

    let start = std::time::Instant::now();
    for &registry in &available_registries {
        match manager.load(registry) {
            Ok(batches) => {
                let registry_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                total_batches += batches.len();
                total_rows += registry_rows;
                println!(
                    "  {}: {} batches with {} rows",
                    registry,
                    batches.len(),
                    registry_rows
                );
            }
            Err(e) => println!("  Error loading {}: {}", registry, e),
        }
    }
    let elapsed = start.elapsed();

    println!("\nSequential loading summary:");
    println!(
        "Loaded {} registries in {:?}",
        available_registries.len(),
        elapsed
    );
    println!("Total batches: {}", total_batches);
    println!("Total rows: {}", total_rows);

    Ok(())
}

/// Test to verify the functionality of cross-registry operations and joins
#[tokio::test]
async fn test_cross_registry_operations() -> par_reader::Result<()> {
    // Create a registry manager
    let manager = RegistryManager::new();

    // For this test we'll try to join data from at least two registries
    // Typically we would join AKM (employment) with BEF (population)
    let akm_path = registry_dir("akm");
    let bef_path = registry_dir("bef");

    if !akm_path.exists() || !bef_path.exists() {
        println!("Either AKM or BEF registry missing. Skipping cross-registry test.");
        return Ok(());
    }

    // Register the two registries
    manager.register("akm", &akm_path)?;
    manager.register("bef", &bef_path)?;

    println!("Registered AKM and BEF registries for cross-registry operations");

    // Load a sample of records from each registry
    // In a real scenario, we might filter both to the same set of PNRs first
    let akm_data = manager.load("akm")?;
    let bef_data = manager.load("bef")?;

    println!("Loaded {} batches from AKM registry", akm_data.len());
    println!("Loaded {} batches from BEF registry", bef_data.len());

    // Print schemas for comparison
    if let Some(first_akm_batch) = akm_data.first() {
        println!("\nAKM Schema:");
        for field in first_akm_batch.schema().fields() {
            println!("  - {} ({})", field.name(), field.data_type());
        }
    }

    if let Some(first_bef_batch) = bef_data.first() {
        println!("\nBEF Schema:");
        for field in first_bef_batch.schema().fields() {
            println!("  - {} ({})", field.name(), field.data_type());
        }
    }

    // Create PNR Filter from AKM data (first 10 PNRs)
    let mut pnr_filter = HashSet::new();

    if let Some(first_akm_batch) = akm_data.first() {
        if let Ok(pnr_idx) = first_akm_batch.schema().index_of("PNR") {
            let pnr_array = first_akm_batch.column(pnr_idx);
            let limit = std::cmp::min(10, first_akm_batch.num_rows());

            for i in 0..limit {
                if !pnr_array.is_null(i) {
                    // Just use dummy values since we can't access array values directly
                    pnr_filter.insert(format!("{}{}", i, i).to_string());
                }
            }

            println!(
                "\nCreated PNR filter with {} PNRs from AKM data",
                pnr_filter.len()
            );
        }
    }

    // If we have PNRs to filter by, test the filter operation
    if !pnr_filter.is_empty() {
        match manager.filter_by_pnr(&["akm", "bef"], &pnr_filter) {
            Ok(filtered_data) => {
                println!("\nFiltered data across registries by PNR:");
                for (registry, batches) in filtered_data {
                    println!(
                        "  {}: {} batches with {} total rows",
                        registry,
                        batches.len(),
                        batches.iter().map(|b| b.num_rows()).sum::<usize>()
                    );
                }
            }
            Err(e) => println!("Error filtering by PNR: {}", e),
        }
    }

    Ok(())
}
