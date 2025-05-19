//! Comprehensive example of loading and using longitudinal data
//!
//! This example demonstrates how to use the longitudinal data loading functionality
//! to load data from various time periods and registries, and how to work with
//! time period information.

use chrono::NaiveDate;
use env_logger;
use log::info;
use par_reader::Individual;
use par_reader::models::core::individual::temporal::TimePeriod;
use par_reader::registry::factory;
use par_reader::registry::temporal_registry_loader::TemporalRegistryLoader;
use par_reader::utils::register::longitudinal::LongitudinalConfig;
use par_reader::utils::register::longitudinal_loader;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::init();

    // Set up the base path to the parquet data
    let base_path = "/home/tkragholm/generated_data/parquet";
    info!("Starting longitudinal data loading example");

    println!("=== Longitudinal Data Example ===");
    println!("This example demonstrates loading and using longitudinal data.\n");

    // Optional: Set date range
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
    let date_range = Some((start_date, end_date));

    // Define the registries we want to load
    let registries = &["bef", "akm", "ind"];

    // SECTION 4: Loading data from multiple registries
    println!("\n=== Section 4: Loading Data from Multiple Registries ===");

    // Load data from selected registries
    println!("Loading data from registries: {:?}", registries);

    // Let's create our own LongitudinalConfig with explicit directory mappings
    let mut config = LongitudinalConfig::new(base_path);

    // Explicitly add registry mappings - use uppercase for registry names since that's what the loader uses
    config.add_registry("AKM", "akm");
    config.add_registry("BEF", "bef");
    config.add_registry("IND", "ind");

    if let Some((start, end)) = date_range {
        config.with_date_range(start, end);
    }

    // Create a custom dataset
    let mut dataset = longitudinal_loader::LongitudinalDataset::new();

    // Keep track of how many individuals we've seen
    let total_individual_count = Arc::new(Mutex::new(0));
    let unique_individual_count = Arc::new(Mutex::new(0));
    
    // Use a thread-safe map to store individuals
    let all_individuals: Arc<Mutex<HashMap<String, Individual>>> = Arc::new(Mutex::new(HashMap::new()));

    // Store registry periods
    let registry_periods: Arc<Mutex<HashMap<String, Vec<TimePeriod>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Process each registry separately for better control
    registries.iter().for_each(|registry_name| {
        println!("  Loading {} registry data...", registry_name);

        // Get registry loader
        let registry_loader = match factory::registry_from_name(registry_name) {
            Ok(loader) => loader,
            Err(e) => {
                println!("  Error creating loader for {}: {}", registry_name, e);
                return;
            }
        };

        // Process time periods one by one instead of all at once
        let registry_name_upper = registry_loader.get_register_name();
        let registry_dir_opt = config.get_registry_dir(registry_name_upper);
        
        if registry_dir_opt.is_none() {
            println!(
                "  No directory found for {} ({})",
                registry_name, registry_name_upper
            );
            return;
        }

        // Clone the PathBuf to avoid ownership issues
        let registry_dir = registry_dir_opt.unwrap().clone();

        // Create a temporal loader
        let temporal_loader = TemporalRegistryLoader::new(
            registry_loader.get_register_name(),
            registry_loader.clone(),
        );

        // Get available time periods
        let time_periods = match temporal_loader.get_available_time_periods(&registry_dir) {
            Ok(periods) => periods,
            Err(e) => {
                println!("  Error getting time periods for {}: {}", registry_name, e);
                return;
            }
        };

        println!(
            "  Found {} time periods for {}",
            time_periods.len(),
            registry_name
        );

        // Store time periods in dataset
        {
            let mut periods = registry_periods.lock().unwrap();
            if !time_periods.is_empty() {
                periods.insert(registry_name.to_string(), time_periods.clone());
            }
        }

        if !time_periods.is_empty() {
            // Create a shared structure to store batch results for each time period
            let period_data: Arc<Mutex<HashMap<TimePeriod, Vec<(PathBuf, usize)>>>> = 
                Arc::new(Mutex::new(HashMap::new()));
            
            // First step: identify all time periods and their file paths
            let period_files: Vec<(TimePeriod, PathBuf)> = time_periods.iter().map(|time_period| {
                let file_path = registry_dir.join(format!("{}.parquet", time_period.to_string()));
                (*time_period, file_path)
            }).collect();
            
            // Process time periods in parallel to load the data
            period_files.par_iter().for_each(|(time_period, file_path)| {
                // Load data for this time period
                if let Ok((_, period_batches)) = temporal_loader.load_time_period(
                    &registry_dir,
                    *time_period,
                    None, // No PNR filter
                ) {
                    // Record the batch size for this time period
                    let mut period_batch_sizes = Vec::new();
                    for batch in &period_batches {
                        period_batch_sizes.push((file_path.clone(), batch.num_rows()));
                    }
                    
                    // Store the batch size information for later processing
                    if !period_batch_sizes.is_empty() {
                        let mut period_data_lock = period_data.lock().unwrap();
                        period_data_lock.insert(*time_period, period_batch_sizes);
                    }
                    
                    // Process individuals from the batches
                    for batch in &period_batches {
                        // Extract individuals from the batch with time period information
                        if let Ok(mut individuals) = Individual::from_batch_with_time_period(
                            batch,
                            file_path,
                            registry_name,
                        ) {
                            let count = individuals.len();
                            *total_individual_count.lock().unwrap() += count;

                            // Update individuals map
                            let mut all_inds = all_individuals.lock().unwrap();
                            for individual in individuals.drain(..) {
                                if !all_inds.contains_key(&individual.pnr) {
                                    *unique_individual_count.lock().unwrap() += 1;
                                }

                                match all_inds.get_mut(&individual.pnr) {
                                    Some(existing) => {
                                        // Update existing individual with data from this registry
                                        existing.merge_fields(&individual);
                                        
                                        // Make sure to merge time period information too
                                        if let Some((registry, period)) = individual.current_time_period {
                                            let source = format!("{}_{}_{}", registry, period.to_string(), "merged");
                                            existing.add_time_period(registry, period, source);
                                        }
                                    }
                                    None => {
                                        // Add new individual
                                        all_inds.insert(individual.pnr.clone(), individual);
                                    }
                                }
                            }
                        }
                    }
                }
            });
            
            // Report period counts
            let period_counts = period_data.lock().unwrap();
            for (period, batch_info) in period_counts.iter() {
                let total_rows: usize = batch_info.iter().map(|(_, size)| *size).sum();
                println!("    Period {}: {} individuals", period.to_string(), total_rows);
            }
        }
    });

    // Transfer individuals to the dataset
    dataset.individuals = all_individuals.lock().unwrap().values().cloned().collect();
    
    // Transfer the registry periods to the dataset
    let periods_map = registry_periods.lock().unwrap().clone();
    for (registry, periods) in periods_map {
        dataset.registry_periods.lock().unwrap().insert(registry, periods);
    }
    
    let total_count = *total_individual_count.lock().unwrap();
    let unique_count = *unique_individual_count.lock().unwrap();
    
    println!(
        "Processed {} total individuals across all registries",
        total_count
    );
    println!("Found {} unique individuals", unique_count);
    println!(
        "Kept {} individuals for analysis",
        dataset.individuals.len()
    );

    // Print dataset information
    println!("\n=== Dataset Information ===");
    println!("Total individuals: {}", dataset.individual_count());
    println!("Registries loaded: {}", dataset.registry_periods.lock().unwrap().len());

    // Print time periods per registry
    println!("\n=== Registry Time Periods ===");
    let registry_periods = dataset.registry_periods.lock().unwrap();
    for (registry, periods) in &*registry_periods {
        println!("Registry '{}': {} time periods", registry, periods.len());

        // Group by year for clearer display
        let mut period_map: BTreeMap<i32, Vec<TimePeriod>> = BTreeMap::new();

        for &period in periods {
            let year = period.year();
            period_map.entry(year).or_default().push(period);
        }

        for (year, year_periods) in &period_map {
            let period_strings: Vec<String> = year_periods
                .iter()
                .map(|p| match p {
                    TimePeriod::Year(_) => "Y".to_string(),
                    TimePeriod::Month(_, m) => format!("M{m:02}"),
                    TimePeriod::Quarter(_, q) => format!("Q{q}"),
                    TimePeriod::Day(d) => format!("D{}", d.format("%m%d")),
                })
                .collect();

            println!("  {}: {}", year, period_strings.join(", "));
        }
    }
    drop(registry_periods); // Release the lock

    // Count individuals with data from multiple registries
    let registry_counts: HashMap<usize, usize> = dataset
        .individuals
        .iter()
        .fold(HashMap::new(), |mut acc, ind| {
            let count = ind.time_periods.len();
            *acc.entry(count).or_insert(0) += 1;
            acc
        });

    println!("\nIndividuals by number of registries:");
    let mut counts: Vec<(&usize, &usize)> = registry_counts.iter().collect();
    counts.sort_by_key(|&(count, _)| *count);

    for (registry_count, individual_count) in counts {
        println!(
            "  {} registries: {} individuals",
            registry_count, individual_count
        );
    }

    // SECTION 5: Working with time-specific data queries
    println!("\n=== Section 5: Time-Specific Data Queries ===");

    // Example: Get all individuals valid at a specific date
    let query_date = NaiveDate::from_ymd_opt(2020, 6, 15).unwrap();
    let individuals_at_date = dataset.individuals_at_date(&query_date);

    println!(
        "Individuals valid on {}: {}",
        query_date,
        individuals_at_date.len()
    );

    // Example: Find individuals with data for a specific time period
    if !dataset.individuals.is_empty() {
        let query_period = TimePeriod::Year(2020);
        let mut individuals_with_period = 0;

        for individual in &dataset.individuals {
            let has_data_for_period = individual
                .time_periods
                .values()
                .any(|periods| periods.contains_key(&query_period));

            if has_data_for_period {
                individuals_with_period += 1;
            }
        }

        println!(
            "Individuals with data for {:?}: {}",
            query_period, individuals_with_period
        );
    }

    // SECTION 6: Historical analysis
    println!("\n=== Section 6: Historical Analysis ===");

    // Example: Look at changes in individual data over time
    if let Some(sample) = dataset.individuals.iter().find(|ind| {
        ind.time_periods.len() > 1 && ind.time_periods.values().next().unwrap().len() > 1
    }) {
        println!("Sample individual with historical data:");
        println!("  PNR: {}", sample.pnr);
        println!("  Gender: {:?}", sample.gender);

        // Print all time periods for this individual
        println!("  Time periods by registry:");
        for (registry, periods) in &sample.time_periods {
            println!("    {}: {:?}", registry, periods.keys());
        }
    } else {
        println!("No individuals found with historical data across multiple time periods");
    }

    println!("\nLongitudinal data loading example completed successfully!");
    Ok(())
}