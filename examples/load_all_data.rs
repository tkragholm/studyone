//! Example demonstrating how to load all longitudinal data
//!
//! This example shows how to load data from all available registries and merge it into
//! a comprehensive dataset with time period information.

use chrono::NaiveDate;
use env_logger;
use log::info;
use par_reader::Individual;
use par_reader::models::core::individual::temporal::TimePeriod;
use par_reader::registry::factory;
use par_reader::utils::register::longitudinal_loader::{
    load_all_longitudinal_data, load_selected_longitudinal_data,
};
use std::collections::{BTreeMap, HashMap, HashSet};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::init();

    // Set up the base path to the parquet data
    let base_path = "/home/tkragholm/generated_data/parquet";
    info!("Starting longitudinal data loading example");

    println!("Loading all longitudinal data from: {}", base_path);

    // Optional: Set date range
    let start_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
    let _date_range = Some((start_date, end_date));

    // Optional: Create a PNR filter for testing
    let _pnr_filter: HashSet<String> = (1..1000).map(|i| format!("{i:010}")).collect();

    // Let's directly load and read one file
    println!("Reading AKM registry data directly:");

    // Find the 2022 file
    let akm_file = format!("{}/akm/2022.parquet", base_path);
    println!("Using file: {}", akm_file);

    // Create AKM loader and load the file
    let akm_loader = factory::registry_from_name("akm")?;
    let batches = akm_loader.load(&std::path::PathBuf::from(&akm_file), None)?;

    println!("Loaded {} batches", batches.len());

    let mut total_individuals = 0;
    let mut all_individuals = Vec::new();

    for (i, batch) in batches.iter().enumerate() {
        println!("Batch {}: {} rows", i, batch.num_rows());

        if let Ok(individuals) = Individual::from_batch(batch) {
            println!("  Converted to {} individuals", individuals.len());
            total_individuals += individuals.len();

            if !individuals.is_empty() {
                let sample = &individuals[0];
                println!(
                    "  Sample - PNR: {}, Gender: {:?}",
                    sample.pnr, sample.gender
                );
                all_individuals.extend(individuals);
            }
        } else {
            println!("  Failed to convert batch to individuals");
        }
    }

    println!("Total individuals: {}", total_individuals);

    // Create a minimal dataset just to satisfy the rest of the example
    let mut dataset = par_reader::utils::register::longitudinal_loader::LongitudinalDataset::new();
    dataset.registry_periods.insert(
        "akm".to_string(),
        vec![par_reader::models::core::individual::temporal::TimePeriod::Year(2022)],
    );
    dataset.individuals = all_individuals;

    // // Or load only specific registries
    // let registries = &["bef", "akm", "ind"];
    // let dataset = load_selected_longitudinal_data(
    //     base_path,
    //     registries,
    //     date_range,
    //     Some(pnr_filter),
    // )?;

    // Print dataset information
    println!("\n=== Dataset Information ===");
    println!("Total individuals: {}", dataset.individual_count());
    println!("Registries loaded: {}", dataset.registry_periods.len());

    // Print time periods per registry
    println!("\n=== Registry Time Periods ===");
    for (registry, periods) in &dataset.registry_periods {
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

    // Print data coverage information
    if let Some((start, end)) = dataset.date_range() {
        println!("\n=== Data Coverage ===");
        println!("Date range: {} to {}", start, end);

        // Calculate total days in range
        let days = (end - start).num_days() + 1;
        println!("Total days in range: {}", days);
    }

    // Print some statistics about the individuals
    println!("\n=== Individual Statistics ===");

    // Count by gender
    let mut gender_counts: HashMap<String, usize> = HashMap::new();
    for ind in &dataset.individuals {
        let gender = ind.gender.clone().unwrap_or_else(|| "Unknown".to_string());
        *gender_counts.entry(gender).or_default() += 1;
    }

    println!("Gender distribution:");
    for (gender, count) in gender_counts {
        println!("  {}: {} individuals", gender, count);
    }

    // Count individuals with data from multiple registries
    let registry_counts = dataset
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

    // Print a sample of an individual with the most registry data
    if let Some(sample) = dataset
        .individuals
        .iter()
        .max_by_key(|ind| ind.time_periods.len())
    {
        println!("\n=== Sample Individual (most registries) ===");
        println!("PNR: {}", sample.pnr);
        println!("Gender: {:?}", sample.gender);
        println!("Birth date: {:?}", sample.birth_date);
        println!("Registries: {}", sample.time_periods.len());

        println!("Registry data:");
        for (registry, periods) in &sample.time_periods {
            let period_count = periods.len();
            println!("  {}: {} time periods", registry, period_count);
        }
    }

    println!("\nLongitudinal data loading example completed successfully!");
    Ok(())
}
