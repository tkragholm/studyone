//! Example of population generation
//!
//! This example demonstrates how to generate a study population using the
//! Population module. It shows the step-by-step process as well as the
//! comprehensive generation method.

use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::algorithm::population::{Population, PopulationBuilder, PopulationConfig};
use crate::error::Result;
use crate::registry::factory;

/// Example of step-by-step population generation
pub fn generate_population_step_by_step(
    bef_path: &Path,
    mfr_path: &Path,
    _pnr_filter: Option<HashSet<String>>,
) -> Result<Population> {
    // Create a population configuration
    let config = PopulationConfig {
        index_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
        min_age: Some(0),
        max_age: Some(18),
        resident_only: true,
        two_parent_only: false,
        study_start_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
        study_end_date: Some(NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()),
    };

    // Initialize the population builder
    let builder = PopulationBuilder::new().with_config(config);

    // Create registry loaders
    let bef_registry = factory::registry_from_name("bef")?;
    let mfr_registry = factory::registry_from_name("mfr")?;

    // Build population step by step
    let builder = builder
        .add_bef_data(&*bef_registry, bef_path)?
        .add_mfr_data(&*mfr_registry, mfr_path)?
        .identify_family_roles();

    // Create the final population
    let mut population = builder.build();

    // Calculate statistics
    population.calculate_statistics();

    // Print summary
    println!("{}", population.print_summary());

    Ok(population)
}

/// Example of comprehensive population generation
pub fn generate_complete_population(
    registry_paths: HashMap<&str, &Path>,
    pnr_filter: Option<HashSet<String>>,
) -> Result<Population> {
    // Create a population configuration
    let config = PopulationConfig {
        index_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
        min_age: Some(0),
        max_age: Some(18),
        resident_only: true,
        two_parent_only: false,
        study_start_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
        study_end_date: Some(NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()),
    };

    // Generate the population using the comprehensive method
    let population = Population::generate_from_registries(config, &registry_paths, pnr_filter)?;

    Ok(population)
}

/// Example of how to use the generated population for case-control matching
pub fn prepare_case_control_matching(population: &Population) {
    // Get case and control families
    let case_families = population.get_case_families();
    let control_families = population.get_control_families();

    println!("Preparing case-control matching:");
    println!("  Number of cases: {}", case_families.len());
    println!("  Number of potential controls: {}", control_families.len());

    // In a real implementation, you would now apply matching criteria
    // For example, using the matching module:
    //
    // let matching_criteria = MatchingCriteria::new()
    //     .with_birth_date_window(30)  // 30 days
    //     .with_same_gender(true)
    //     .with_family_size_window(1);
    //
    // let matched_pairs = matcher.match_case_control(
    //     &case_families,
    //     &control_families,
    //     &matching_criteria,
    //     3,  // 3:1 matching ratio
    // );
}

/// Main example function
pub fn run_population_example() -> Result<()> {
    // Define paths to registry data
    let bef_path = Path::new("data/bef/bef_2020.parquet");
    let mfr_path = Path::new("data/mfr/mfr_2020.parquet");
    let vnds_path = Path::new("data/vnds/vnds_2020.parquet");
    let dod_path = Path::new("data/dod/dod_2020.parquet");
    let lpr_path = Path::new("data/lpr/lpr_diag_2020.parquet");
    let ind_path = Path::new("data/ind/ind_2020.parquet");

    // Create optional PNR filter (example: focus on a specific set of individuals)
    let mut pnr_filter = HashSet::new();
    pnr_filter.insert("0123456789".to_string());
    pnr_filter.insert("9876543210".to_string());

    // Example 1: Step-by-step generation
    println!("Example 1: Step-by-step population generation");
    let _population1 =
        generate_population_step_by_step(bef_path, mfr_path, Some(pnr_filter.clone()))?;

    // Example 2: Comprehensive generation
    println!("\nExample 2: Comprehensive population generation");

    // Create a map of registry paths
    let mut registry_paths = HashMap::new();
    registry_paths.insert("bef", bef_path);
    registry_paths.insert("mfr", mfr_path);
    registry_paths.insert("vnds", vnds_path);
    registry_paths.insert("dod", dod_path);
    registry_paths.insert("lpr", lpr_path);
    registry_paths.insert("ind", ind_path);

    let population2 = generate_complete_population(registry_paths, Some(pnr_filter))?;

    // Example 3: Preparing for case-control matching
    println!("\nExample 3: Preparing for case-control matching");
    prepare_case_control_matching(&population2);

    Ok(())
}
