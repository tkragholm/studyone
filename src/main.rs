use par_reader::Result;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Datelike;
use chrono::NaiveDate;
use log::info;

use par_reader::algorithm::health::lpr_integration::{LprConfig, integrate_lpr2_components};
use par_reader::algorithm::health::scd::{self, ScdConfig};
use par_reader::algorithm::matching::balance::BalanceCalculator;
use par_reader::algorithm::matching::criteria::{MatchingConfig, MatchingCriteria};
use par_reader::algorithm::matching::matcher::Matcher;
use par_reader::algorithm::population::{Population, PopulationBuilder, PopulationConfig};

use par_reader::models::diagnosis::DiagnosisCollection;
use par_reader::registry::factory;

use par_reader::utils::test_utils::{
    ensure_path_exists, get_available_year_files, registry_dir, test_data_dir,
};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Skip test if test data directory doesn't exist
    let data_dir = test_data_dir();
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
    let diagnosis_collection = load_real_diagnoses(&population)?;

    // Count total diagnoses
    let mut total_diagnoses = 0;
    for individual in population.collection.get_individuals() {
        total_diagnoses += diagnosis_collection.get_diagnoses(&individual.pnr).len();
    }
    println!("Loaded {} diagnoses", total_diagnoses);

    // Apply SCD algorithm to diagnoses
    let birth_dates = collect_birth_dates(&population);

    let scd_config = ScdConfig {
        start_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
        end_date: Some(NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()),
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

/// Generate a test population from the test data
fn generate_test_population() -> Result<Population> {
    // Create a population configuration
    let config = PopulationConfig {
        index_date: NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(),
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

    // Get paths to the test data
    let bef_path = registry_dir("bef");
    let mfr_path = registry_dir("mfr");

    ensure_path_exists(&bef_path)?;
    ensure_path_exists(&mfr_path)?;

    // Build population step by step
    let builder = builder
        .add_bef_data(&*bef_registry, &bef_path)?
        .add_mfr_data(&*mfr_registry, &mfr_path)?
        .identify_family_roles();

    // Create the final population
    let mut population = builder.build();

    // Calculate statistics
    population.calculate_statistics();

    Ok(population)
}

/// Collect birth dates from the population
fn collect_birth_dates(population: &Population) -> HashMap<String, NaiveDate> {
    let mut birth_dates = HashMap::new();

    // Extract individuals from the family collection
    for individual in population.collection.get_individuals() {
        if let Some(birthdate) = individual.birth_date {
            birth_dates.insert(individual.pnr.clone(), birthdate);
        }
    }

    birth_dates
}

/// Load diagnoses from real LPR test data for all available years
fn load_real_diagnoses(population: &Population) -> Result<DiagnosisCollection> {
    // Check if LPR data directories exist
    let lpr_diag_path = registry_dir("lpr_diag");
    let lpr_adm_path = registry_dir("lpr_adm");
    
    // Check if at least one of the LPR directories exists
    if !lpr_diag_path.exists() && !lpr_adm_path.exists() {
        return Err(anyhow::anyhow!(
            "LPR data directories not found. Need either lpr_diag or lpr_adm."
        ).into());
    }

    // Create LPR registries
    let lpr_diag_registry = factory::registry_from_name("lpr_diag")?;
    let lpr_adm_registry = factory::registry_from_name("lpr_adm")?;

    // Get all available LPR files
    let lpr_diag_files = get_lpr_diag_files()?;
    let lpr_adm_files = get_lpr_adm_files()?;

    // Log what we found to help debug
    info!("Found {} LPR_DIAG files", lpr_diag_files.len());
    info!("Found {} LPR_ADM files", lpr_adm_files.len());
    
    if lpr_diag_files.is_empty() || lpr_adm_files.is_empty() {
        info!("Not enough LPR files found. Will skip diagnosis processing.");
        // Return empty diagnosis collection instead of error
        return Ok(DiagnosisCollection::new());
    }

    // Extract PNRs from the population to use as filter
    let pnrs: HashSet<String> = population
        .collection
        .get_individuals()
        .iter()
        .map(|individual| individual.pnr.clone())
        .collect();

    // Create a PNR filter
    let pnr_filter = Some(pnrs);

    // Create a combined diagnosis collection to store all diagnoses
    let mut combined_diagnosis_collection = DiagnosisCollection::new();
    let lpr_config = LprConfig::default();

    // Display file count for user information
    info!(
        "Found {} LPR_DIAG files and {} LPR_ADM files to process",
        lpr_diag_files.len(),
        lpr_adm_files.len()
    );

    // Get all PNRs in the population for iterating later
    let all_pnrs: Vec<String> = population
        .collection
        .get_individuals()
        .iter()
        .map(|individual| individual.pnr.clone())
        .collect();

    // Match LPR_DIAG and LPR_ADM files by year
    for (diag_idx, diag_file) in lpr_diag_files.iter().enumerate() {
        // Try to find matching ADM file by getting the same index
        if diag_idx >= lpr_adm_files.len() {
            info!("No matching LPR_ADM file for {:?}, skipping", diag_file);
            continue;
        }

        let adm_file = &lpr_adm_files[diag_idx];

        // Extract year from filenames for logging
        let diag_year = diag_file
            .file_stem()
            .and_then(|name| name.to_string_lossy().parse::<u32>().ok())
            .unwrap_or(0);

        info!(
            "Processing year {} - DIAG: {:?}, ADM: {:?}",
            diag_year,
            diag_file.file_name().unwrap_or_default(),
            adm_file.file_name().unwrap_or_default()
        );

        // Load data for this year
        let lpr_diag_data = lpr_diag_registry.load(diag_file, pnr_filter.as_ref())?;
        let lpr_adm_data = lpr_adm_registry.load(adm_file, pnr_filter.as_ref())?;

        // Skip if empty data
        if lpr_diag_data.is_empty() || lpr_adm_data.is_empty() {
            info!("No data for year {}, skipping", diag_year);
            continue;
        }

        // Process this year's data
        let year_diagnoses = integrate_lpr2_components(
            &lpr_adm_data[0],  // First batch
            &lpr_diag_data[0], // First batch
            None,              // No LPR_BES data
            &lpr_config,
        )?;

        // Count diagnoses in this batch
        let mut diagnoses_count = 0;

        // Add diagnoses to combined collection by looking up each PNR
        for pnr in &all_pnrs {
            let diagnoses = year_diagnoses.get_diagnoses(pnr);
            for diagnosis in diagnoses {
                combined_diagnosis_collection.add_diagnosis(diagnosis.as_ref().clone());
                diagnoses_count += 1;
            }
        }

        info!(
            "Added {} diagnoses from year {}",
            diagnoses_count, diag_year
        );
    }

    // Count total diagnoses by iterating through all PNRs
    let mut total_diagnoses = 0;
    for pnr in &all_pnrs {
        total_diagnoses += combined_diagnosis_collection.get_diagnoses(pnr).len();
    }

    // Check if we loaded any diagnoses
    if total_diagnoses == 0 {
        return Err(anyhow::anyhow!("No LPR data loaded from any year").into());
    }

    info!("Total diagnoses loaded from all years: {}", total_diagnoses);

    Ok(combined_diagnosis_collection)
}

/// Get available LPR_DIAG files
fn get_lpr_diag_files() -> Result<Vec<PathBuf>> {
    let lpr_diag_path = registry_dir("lpr_diag");
    if !lpr_diag_path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr_diag"))
}

/// Get available LPR_ADM files
fn get_lpr_adm_files() -> Result<Vec<PathBuf>> {
    let lpr_adm_path = registry_dir("lpr_adm");
    if !lpr_adm_path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr_adm"))
}

/// Prepare case and control groups from the population
fn prepare_case_control_groups(
    population: &Population,
    individuals_with_scd: &[String],
) -> Result<(
    arrow::record_batch::RecordBatch,
    arrow::record_batch::RecordBatch,
)> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, StringBuilder, UInt8Builder,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    // Convert individuals to record batches
    let scd_set: HashSet<String> = individuals_with_scd.iter().cloned().collect();

    // Split individuals into cases and controls
    let mut cases = Vec::new();
    let mut controls = Vec::new();

    for individual in population.collection.get_individuals() {
        if scd_set.contains(&individual.pnr) {
            cases.push(individual.clone());
        } else {
            controls.push(individual.clone());
        }
    }

    // Define schema for record batches
    let schema = Arc::new(Schema::new(vec![
        Field::new("pnr", DataType::Utf8, false),
        Field::new("birthdate", DataType::Date32, true),
        Field::new("gender", DataType::UInt8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("is_rural", DataType::Boolean, true),
        Field::new("education_level", DataType::UInt8, true),
        Field::new("municipality_code", DataType::Utf8, true),
        Field::new("income", DataType::Float64, true),
    ]));

    // Helper function to convert individuals to a record batch
    let convert_to_batch =
        |individuals: Vec<Arc<par_reader::models::Individual>>| -> Result<RecordBatch> {
            // Create array builders
            let mut pnr_builder = StringBuilder::new();
            let mut birthdate_builder = Date32Builder::new();
            let mut gender_builder = UInt8Builder::new();
            let mut age_builder = Int32Builder::new();
            let mut is_rural_builder = BooleanBuilder::new();
            let mut education_builder = UInt8Builder::new();
            let mut municipality_builder = StringBuilder::new();
            let mut income_builder = Float64Builder::new();

            // Add data
            for individual in individuals {
                // PNR (required)
                pnr_builder.append_value(&individual.pnr);

                // Birthdate
                if let Some(date) = individual.birth_date {
                    let days_since_epoch = date
                        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32;
                    birthdate_builder.append_value(days_since_epoch);
                } else {
                    birthdate_builder.append_null();
                }

                // Gender
                gender_builder.append_value(individual.gender as u8);

                // Age (calculated from birthdate and the study index date)
                if let Some(birthdate) = individual.birth_date {
                    let index_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(); // Same as population config
                    let age = index_date.year() - birthdate.year();
                    if index_date.ordinal() < birthdate.ordinal() {
                        age_builder.append_value(age - 1);
                    } else {
                        age_builder.append_value(age);
                    }
                } else {
                    age_builder.append_null();
                }

                // Rural status
                is_rural_builder.append_value(individual.is_rural);

                // Education level
                education_builder.append_value(individual.education_level as u8);

                // Municipality code
                if let Some(code) = &individual.municipality_code {
                    municipality_builder.append_value(code);
                } else {
                    municipality_builder.append_null();
                }

                // Income (optional)
                income_builder.append_null(); // We don't have income in our test data
            }

            // Create arrays
            let arrays = vec![
                Arc::new(pnr_builder.finish()) as _,
                Arc::new(birthdate_builder.finish()) as _,
                Arc::new(gender_builder.finish()) as _,
                Arc::new(age_builder.finish()) as _,
                Arc::new(is_rural_builder.finish()) as _,
                Arc::new(education_builder.finish()) as _,
                Arc::new(municipality_builder.finish()) as _,
                Arc::new(income_builder.finish()) as _,
            ];

            // Create record batch
            RecordBatch::try_new(schema.clone(), arrays)
                .map_err(|e| anyhow::anyhow!("Failed to create record batch: {}", e).into())
        };

    // Convert cases and controls to record batches
    let case_batch = convert_to_batch(cases)?;
    let control_batch = convert_to_batch(controls)?;

    Ok((case_batch, control_batch))
}
