//! Examples demonstrating IND registry direct model integration
//!
//! These examples show how to use direct registry-to-model conversion 
//! for Income data from the IND registry.

use std::collections::HashMap;
use std::path::Path;

use crate::error::Result;
use crate::models::income::{Income, IncomeTrajectory};
use crate::registry::ind::IndRegister;
use crate::registry::ind_model_conversion::YearConfiguredIndRegister;
use crate::registry::model_conversion::ModelConversionExt;

/// Load income data directly from IND registry with default year (2020)
pub fn load_incomes_example(base_path: &Path) -> Result<()> {
    let ind_registry = IndRegister::new();
    
    // Load incomes for all individuals
    let incomes = ind_registry.load_as::<Income>(base_path, None)?;
    
    println!("Loaded {} income records from IND registry", incomes.len());
    
    // Print a sample of the data
    for (i, income) in incomes.iter().take(5).enumerate() {
        println!(
            "Income {}: PNR={}, Year={}, Type={}, Amount={}",
            i + 1,
            income.individual_pnr,
            income.year,
            income.income_type,
            income.amount
        );
    }
    
    Ok(())
}

/// Load income data using year-configured registry with inflation adjustment
pub fn load_inflation_adjusted_incomes_example(base_path: &Path) -> Result<()> {
    // Create CPI index for inflation adjustment
    // These are example values - in a real application, these would come from official statistics
    let mut cpi_indices = HashMap::new();
    cpi_indices.insert(2015, 0.91);
    cpi_indices.insert(2016, 0.92);
    cpi_indices.insert(2017, 0.94);
    cpi_indices.insert(2018, 0.96);
    cpi_indices.insert(2019, 0.98);
    cpi_indices.insert(2020, 1.00);
    cpi_indices.insert(2021, 1.03);
    cpi_indices.insert(2022, 1.08);
    
    // Create year-configured registry for 2019 data with inflation adjustment
    let ind_registry = YearConfiguredIndRegister::new(2019)
        .with_cpi_indices(cpi_indices);
    
    // Load incomes for all individuals
    let incomes = ind_registry.load_as::<Income>(base_path, None)?;
    
    println!(
        "Loaded {} inflation-adjusted income records from IND registry (year 2019)",
        incomes.len()
    );
    
    Ok(())
}

/// Create income trajectories directly from IND data over multiple years
pub fn create_income_trajectories_example(base_path: &Path) -> Result<()> {
    // Initialize HashMap to store trajectories by individual and income type
    let mut trajectories: HashMap<(String, String), IncomeTrajectory> = HashMap::new();
    
    // Load data for multiple years
    for year in 2015..=2020 {
        let ind_registry = YearConfiguredIndRegister::new(year);
        let incomes = ind_registry.load_as::<Income>(base_path, None)?;
        
        // Add each income to appropriate trajectory
        for income in incomes {
            let key = (income.individual_pnr.clone(), income.income_type.clone());
            
            // Get or create trajectory
            let trajectory = trajectories
                .entry(key.clone())
                .or_insert_with(|| IncomeTrajectory::new(key.0.clone(), key.1.clone()));
            
            // Add income to trajectory
            trajectory.add_income(income.year, income.amount);
        }
    }
    
    println!("Created {} income trajectories", trajectories.len());
    
    // Print some statistics from the trajectories
    let mut count = 0;
    for ((pnr, income_type), trajectory) in trajectories.iter().take(5) {
        if let Some(mean) = trajectory.mean_income() {
            println!(
                "Trajectory {}: PNR={}, Type={}, Years={}, Mean Income={}",
                count + 1,
                pnr,
                income_type,
                trajectory.years().len(),
                mean
            );
            count += 1;
        }
    }
    
    Ok(())
}

/// Demonstrating async loading of income data
pub async fn load_incomes_async_example(base_path: &Path) -> Result<()> {
    let ind_registry = IndRegister::new();
    
    // Load incomes asynchronously
    let incomes = ind_registry.load_as_async::<Income>(base_path, None).await?;
    
    println!("Asynchronously loaded {} income records from IND registry", incomes.len());
    
    Ok(())
}