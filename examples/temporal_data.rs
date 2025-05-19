//! Example showing how to work with temporal registry data
//!
//! This example demonstrates how to load and process registry data
//! that spans multiple time periods, handling the time information
//! in the filenames.

use chrono::NaiveDate;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use par_reader::error::Result;
use par_reader::models::core::Individual;
use par_reader::models::core::individual::temporal::TimePeriod;
use par_reader::registry::direct_registry_loader::DirectRegistryLoader;
use par_reader::registry::temporal_registry_loader::TemporalRegistryLoader;
use par_reader::utils::io::paths::temporal::get_registry_time_period_files;

fn main() -> Result<()> {
    // Set up logging
    env_logger::init();

    // Define registry directories
    let akm_path = Path::new("/path/to/AKM");
    let bef_path = Path::new("/path/to/BEF");
    let ind_path = Path::new("/path/to/IND");

    println!("Loading temporal registry data...");

    // Create inner loaders
    let akm_inner = Arc::new(DirectRegistryLoader::new("AKM"));
    let bef_inner = Arc::new(DirectRegistryLoader::new("BEF"));
    let ind_inner = Arc::new(DirectRegistryLoader::new("IND"));

    // Create temporal loaders
    let akm_loader = TemporalRegistryLoader::new("AKM", akm_inner);
    let bef_loader = TemporalRegistryLoader::new("BEF", bef_inner);
    let ind_loader = TemporalRegistryLoader::new("IND", ind_inner);

    // Get available time periods for each registry
    let akm_periods = akm_loader.get_available_time_periods(akm_path)?;
    let bef_periods = bef_loader.get_available_time_periods(bef_path)?;
    let ind_periods = ind_loader.get_available_time_periods(ind_path)?;

    println!("Available time periods:");
    println!("AKM: {:?}", akm_periods);
    println!("BEF: {:?}", bef_periods);
    println!("IND: {:?}", ind_periods);

    // Load data for specific time periods
    let year_2018 = TimePeriod::Year(2018);
    let jan_2018 = TimePeriod::Month(2018, 1);

    // Get AKM data for 2018
    let (akm_period, akm_batches) = akm_loader.load_time_period(akm_path, year_2018, None)?;
    println!(
        "Loaded {} AKM batches for {:?}",
        akm_batches.len(),
        akm_period
    );

    // Get BEF data for January 2018
    let (bef_period, bef_batches) = bef_loader.load_time_period(bef_path, jan_2018, None)?;
    println!(
        "Loaded {} BEF batches for {:?}",
        bef_batches.len(),
        bef_period
    );

    // Process individuals with temporal data
    process_individuals(&akm_batches, &bef_batches)?;

    // Example of loading multiple time periods
    let multiple_years = vec![
        TimePeriod::Year(2017),
        TimePeriod::Year(2018),
        TimePeriod::Year(2019),
    ];

    let akm_multi_periods = akm_loader.load_time_periods(akm_path, &multiple_years, None)?;

    println!("Multi-period results:");
    for (period, batches) in &akm_multi_periods {
        println!("Period {:?}: {} batches", period, batches.len());
    }

    // Example time range filtering
    let start_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2019, 12, 31).unwrap();

    println!(
        "Filtering data for time range: {} to {}",
        start_date, end_date
    );

    // Get temporal files
    let bef_files = get_registry_time_period_files(bef_path)?;

    // Filter to the date range
    let filtered_files = par_reader::utils::io::paths::temporal::filter_files_by_date_range(
        &bef_files, start_date, end_date,
    );

    println!("Filtered files: {}", filtered_files.len());
    for (period, path) in filtered_files {
        println!("  {:?}: {}", period, path.display());
    }

    Ok(())
}

/// Process individuals with temporal data
fn process_individuals(
    akm_batches: &[par_reader::RecordBatch],
    bef_batches: &[par_reader::RecordBatch],
) -> Result<()> {
    // Create a map to store individuals
    let mut individuals = HashMap::new();

    // Process AKM data (yearly)
    for batch in akm_batches {
        for row in 0..batch.num_rows() {
            // Extract PNR (just a placeholder - in real code you'd get this from the batch)
            let pnr = format!("PNR_{}", row);

            // Get or create individual
            let individual = individuals
                .entry(pnr.clone())
                .or_insert_with(|| Individual::new(pnr, None));

            // Enhance with AKM data for 2018
            individual.enhance_from_registry(batch, row, "AKM", Some(TimePeriod::Year(2018)))?;
        }
    }

    // Process BEF data (monthly)
    for batch in bef_batches {
        for row in 0..batch.num_rows() {
            // Extract PNR (just a placeholder - in real code you'd get this from the batch)
            let pnr = format!("PNR_{}", row);

            // Get or create individual
            let individual = individuals
                .entry(pnr.clone())
                .or_insert_with(|| Individual::new(pnr, None));

            // Enhance with BEF data for January 2018
            individual.enhance_from_registry(
                batch,
                row,
                "BEF",
                Some(TimePeriod::Month(2018, 1)),
            )?;
        }
    }

    // Display time period information for each individual
    for (pnr, individual) in &individuals {
        println!("Individual: {}", pnr);

        // Get all registries with time period data
        for (registry, periods) in &individual.time_periods {
            println!("  Registry: {}", registry);

            // Display all time periods for this registry
            for (period, source) in periods {
                println!("    {:?} - Source: {}", period, source);
            }
        }

        // Example: Get the latest AKM data
        if let Some(latest_akm) = individual.get_latest_time_period("AKM") {
            println!("  Latest AKM data: {:?}", latest_akm);
        }

        // Example: Find all data sources for a specific date
        let query_date = NaiveDate::from_ymd_opt(2018, 1, 15).unwrap();
        let sources = individual.get_data_sources_for_date(query_date);

        println!("  Data sources for {}: ", query_date);
        for (registry, periods) in &sources {
            println!("    {}: {:?}", registry, periods);
        }
    }

    Ok(())
}
