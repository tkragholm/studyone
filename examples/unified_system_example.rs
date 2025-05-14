//! Example demonstrating the unified schema system
//!
//! This module provides an example of how to use the unified schema system
//! for loading and processing registry data.

use crate::Result;
use crate::registry::unified as unified_factory;
use crate::models::core::individual::base::Individual;
use log::info;
use std::path::Path;

/// Run example using the unified schema system
///
/// This function demonstrates loading registry data using the unified schema system.
///
/// # Arguments
/// * `base_dir` - Base directory containing the registry data
/// * `start_date` - Start date for filtering data
/// * `end_date` - End date for filtering data
///
/// # Returns
/// * `Result<usize>` - Number of individuals loaded
pub async fn run_unified_system_example(
    base_dir: &Path,
    start_date: chrono::NaiveDate,
    end_date: chrono::NaiveDate,
) -> Result<usize> {
    info!("Running registry example with the unified schema system");
    info!("Loading data from: {}", base_dir.display());
    info!("Date range: {} to {}", start_date, end_date);

    // Define registry paths
    let akm_path = base_dir.join("akm");
    let bef_path = base_dir.join("bef");
    let ind_path = base_dir.join("ind");
    let lpr_adm_path = base_dir.join("lpr_adm");
    let lpr_diag_path = base_dir.join("lpr_diag");

    // Load data from all registries using the unified factory
    let base_paths = [
        ("akm", akm_path.as_path()),
        ("bef", bef_path.as_path()),
        ("ind", ind_path.as_path()),
        ("lpr_adm", lpr_adm_path.as_path()),
        ("lpr_diag", lpr_diag_path.as_path()),
    ];

    // Use the unified factory to load all registries asynchronously
    let all_batches = unified_factory::load_multiple_registries_async(&base_paths, None).await?;

    // Process the data (e.g., convert to domain models)
    let individuals: Vec<Individual> = Vec::new();
    // ... implementation details for processing the data ...

    info!("Loaded {} individuals using the unified schema system", individuals.len());

    Ok(individuals.len())
}

/// Entry point for running the unified system example
pub fn main() -> Result<()> {
    let base_dir = Path::new("path/to/your/data");
    let start_date = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let end_date = chrono::NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run_unified_system_example(base_dir, start_date, end_date))?;

    Ok(())
}