use par_reader::Result;

use log::info;

use par_reader::algorithm::health::lpr_integration::{self};
use par_reader::algorithm::health::scd::{self, ScdConfig};
use par_reader::algorithm::matching::balance::BalanceCalculator;
use par_reader::algorithm::matching::criteria::{MatchingConfig, MatchingCriteria};
use par_reader::algorithm::matching::matcher::Matcher;
use par_reader::algorithm::matching::prepare_case_control_groups;
use par_reader::algorithm::population::core::generate_test_population;
use par_reader::utils::registry_utils::collect_birth_dates;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Skip test if test data directory doesn't exist
    let data_dir = par_reader::utils::test_utils::test_data_dir();
    if !data_dir.exists() {
        println!(
            "Test data directory not found, skipping test: {}",
            data_dir.display()
        );
        return Ok(());
    }

    // Step 1: Create a population from test data
    info!("Step 1: Creating population from test data");
    let population = generate_test_population()?;
    println!("Population created: {}", population.print_summary());

    // Step 2: Load diagnoses and identify SCD cases
    info!("Step 2: Loading diagnoses and identifying SCD cases");
    let diagnosis_collection = lpr_integration::load_real_diagnoses(&population)?;

    // Count total diagnoses
    let mut total_diagnoses = 0;
    for individual in population.collection.get_individuals() {
        total_diagnoses += diagnosis_collection.get_diagnoses(&individual.pnr).len();
    }
    println!("Loaded {total_diagnoses} diagnoses");

    // Apply SCD algorithm to diagnoses
    let birth_dates = collect_birth_dates(&population);

    let scd_config = ScdConfig {
        start_date: Some(chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
        end_date: Some(chrono::NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()),
        include_congenital: true,
        min_age_years: None,
        max_age_years: None,
    };

    let scd_results = scd::apply_scd_algorithm(&diagnosis_collection, &scd_config, &birth_dates)?;

    // Extract individuals with SCD
    let individuals_with_scd = scd::get_individuals_with_scd(&scd_results);
    println!("Found {} individuals with SCD", individuals_with_scd.len());

    // Skip the rest of the test if we don't have enough SCD cases
    if individuals_with_scd.len() < 5 {
        println!("Not enough SCD cases found, skipping matching steps");
        return Ok(());
    }

    // Step 3: Convert population into case-control groups for matching
    info!("Step 3: Preparing case and control groups");
    let (cases, controls) = prepare_case_control_groups(&population, &individuals_with_scd)?;

    println!(
        "Prepared {} cases and {} controls for matching",
        cases.num_rows(),
        controls.num_rows()
    );

    // Step 4: Perform matching
    info!("Step 4: Performing case-control matching");

    // Configure matching criteria and config
    let criteria = MatchingCriteria {
        birth_date_window_days: 30,
        parent_birth_date_window_days: 365,
        require_both_parents: false,
        require_same_gender: true,
        match_family_size: true,
        family_size_tolerance: 1,
        match_education_level: true,
        match_geography: true,
        match_parental_status: true,
        match_immigrant_background: false,
    };

    let mut matching_config = MatchingConfig::default();
    matching_config.criteria = criteria;
    matching_config.matching_ratio = 3; // 3:1 matching

    let matcher = Matcher::new(matching_config);
    let matching_result = matcher.perform_matching(&cases, &controls)?;

    println!(
        "Matched {} cases with {} controls",
        matching_result.matched_case_count, matching_result.matched_control_count,
    );

    if matching_result.matched_case_count > 0 {
        println!(
            "Matching ratio: {}:1",
            matching_result.matched_control_count / matching_result.matched_case_count
        );
    }

    // Step 5: Check covariate balance
    info!("Step 5: Calculating covariate balance");
    let balance_calculator = BalanceCalculator::new()
        .with_exclude_columns(vec!["pnr".to_string(), "birthdate".to_string()]);

    let balance_report = balance_calculator.calculate_balance(
        &matching_result.matched_cases,
        &matching_result.matched_controls,
    )?;

    println!("Balance report:\n{}", balance_report.to_string());

    // Check if matching was successful
    assert!(
        matching_result.matched_case_count > 0,
        "No cases were matched"
    );
    assert!(
        matching_result.matched_control_count > 0,
        "No controls were matched"
    );

    Ok(())
}