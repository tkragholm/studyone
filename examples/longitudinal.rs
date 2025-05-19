//! Example demonstrating longitudinal data handling
//!
//! This example shows how to work with longitudinal registry data,
//! where data is provided in different files for different time periods.

use chrono::NaiveDate;
use par_reader::models::core::individual::temporal::TimePeriod;
use par_reader::registry::factory;
use par_reader::utils::register::longitudinal::{
    LongitudinalConfig, detect_registry_time_periods, load_longitudinal_data,
    merge_temporal_individuals,
};
use std::collections::{BTreeMap, HashMap, HashSet};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the base path to the parquet data
    let base_path = "/home/tkragholm/generated_data/parquet";

    println!("Analyzing longitudinal registry data in: {}", base_path);

    // 1. Detect available registry time periods
    println!("\n=== Detecting available registry time periods ===");
    let registry_periods = detect_registry_time_periods(base_path)?;

    for (registry, periods) in &registry_periods {
        println!(
            "Registry '{}' has {} time periods:",
            registry,
            periods.len()
        );

        let mut period_map: BTreeMap<i32, Vec<TimePeriod>> = BTreeMap::new();

        // Group by year for display
        for &period in periods {
            let year = period.year();
            period_map.entry(year).or_default().push(period);
        }

        // Display grouped by year
        for (year, year_periods) in &period_map {
            print!("  {}: ", year);

            let period_strings: Vec<String> = year_periods
                .iter()
                .map(|p| match p {
                    TimePeriod::Year(y) => format!("{y}"),
                    TimePeriod::Month(_, m) => format!("M{m:02}"),
                    TimePeriod::Quarter(_, q) => format!("Q{q}"),
                    TimePeriod::Day(d) => format!("{}", d.format("%m-%d")),
                })
                .collect();

            println!("{}", period_strings.join(", "));
        }
    }

    // 2. Configure the longitudinal data loading
    println!("\n=== Configuring longitudinal data loading ===");
    let mut config = LongitudinalConfig::new(base_path);

    // Map registry names to directory names
    for registry in registry_periods.keys() {
        config.add_registry(registry, registry);
        println!("Added registry mapping: {} -> {}", registry, registry);
    }

    // Set date range (if needed)
    let start_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
    config.with_date_range(start_date, end_date);
    println!("Set date range: {} to {}", start_date, end_date);

    // Create a PNR filter subset for demonstration
    let pnr_filter: HashSet<String> = (1..1000).map(|i| format!("{i:010}")).collect();
    config.with_pnr_filter(pnr_filter);
    println!(
        "Set PNR filter with {} PNRs",
        config.pnr_filter.as_ref().unwrap().len()
    );

    // 3. Choose a registry to work with
    let selected_registry = "bef";
    println!("\n=== Working with '{}' registry ===", selected_registry);

    // Create a registry loader
    let registry_loader = factory::registry_from_name(selected_registry)?;

    // 4. Load longitudinal data
    println!("Loading longitudinal data...");
    let temporal_data = load_longitudinal_data(registry_loader, &config)?;

    // 5. Print statistics about the data
    println!("\n=== Temporal data statistics ===");
    let mut total_individuals = 0;

    for (period, individuals) in &temporal_data {
        total_individuals += individuals.len();
        println!(
            "Period {}: {} individuals",
            period.to_string(),
            individuals.len()
        );

        // Print first individual info (as a sample) if there's any data
        if !individuals.is_empty() {
            let sample = &individuals[0];
            println!(
                "  Sample: PNR={}, Gender={:?}, Birth date={:?}, time periods={}",
                sample.pnr,
                sample.gender,
                sample.birth_date,
                sample.time_periods.len(),
            );
        }
    }

    // 6. Merge individuals from different time periods
    println!("\n=== Merging individuals across time periods ===");
    let merged_individuals = merge_temporal_individuals(&temporal_data);
    println!(
        "Merged {} time periods into {} unique individuals",
        temporal_data.len(),
        merged_individuals.len()
    );

    // 7. Analyze time span of data
    if !temporal_data.is_empty() {
        let first_period = *temporal_data.keys().next().unwrap();
        let last_period = *temporal_data.keys().last().unwrap();

        println!("\n=== Time span analysis ===");
        println!("Earliest period: {}", first_period.to_string());
        println!("Latest period: {}", last_period.to_string());
        println!(
            "Date range: {} to {}",
            first_period.start_date().format("%Y-%m-%d"),
            last_period.end_date().format("%Y-%m-%d")
        );
    }

    // 8. Demonstrate accessing individuals from a specific time period
    if !temporal_data.is_empty() {
        println!("\n=== Accessing specific time period data ===");
        let latest_period = *temporal_data.keys().last().unwrap();
        let latest_individuals = &temporal_data[&latest_period];

        println!(
            "Latest period ({}): {} individuals",
            latest_period.to_string(),
            latest_individuals.len()
        );

        // Count individuals by gender
        let mut gender_counts: HashMap<String, usize> = HashMap::new();

        for ind in latest_individuals {
            if let Some(gender) = &ind.gender {
                *gender_counts.entry(gender.clone()).or_default() += 1;
            } else {
                *gender_counts.entry("Unknown".to_string()).or_default() += 1;
            }
        }

        println!("Gender distribution:");
        for (gender, count) in gender_counts {
            println!("  {}: {} individuals", gender, count);
        }
    }

    println!("\nLongitudinal data example completed successfully!");
    Ok(())
}
