//! Balance checking utilities for Record Batches
//!
//! This module provides functions for checking balance between case and control groups.

use crate::error::{IdsError, Result};
use crate::algorithm::statistics::{mean, std_dev};
use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Metric for balance between cases and controls
#[derive(Debug, Clone)]
pub struct BalanceMetric {
    /// Name of the covariate (column)
    pub name: String,
    
    /// Standardized difference
    pub standardized_difference: f64,
    
    /// Mean for cases
    pub case_mean: f64,
    
    /// Mean for controls
    pub control_mean: f64,
    
    /// Standard deviation for cases
    pub case_std: f64,
    
    /// Standard deviation for controls
    pub control_std: f64,
    
    /// Whether the covariate is categorical
    pub categorical: bool,
}

/// Report on balance between cases and controls
#[derive(Debug, Clone)]
pub struct BalanceReport {
    /// Balance metrics
    pub metrics: Vec<BalanceMetric>,
    
    /// Summary statistics
    pub summary: BalanceSummary,
}

/// Summary statistics for a balance report
#[derive(Debug, Clone)]
pub struct BalanceSummary {
    /// Number of covariates with standardized difference > 0.1
    pub imbalanced_covariates: usize,
    
    /// Maximum standardized difference
    pub max_standardized_difference: f64,
    
    /// Mean absolute standardized difference
    pub mean_absolute_standardized_difference: f64,
    
    /// Total number of covariates
    pub total_covariates: usize,
}

/// Load records from a parquet file
pub fn load_records(path: &str) -> Result<Vec<RecordBatch>> {
    use std::path::Path;
    use tokio::runtime::Runtime;
    
    let path = Path::new(path);
    if !path.exists() {
        return Err(IdsError::Validation(format!("File does not exist: {}", path.display())));
    }
    
    // Create a tokio runtime for async operations
    let runtime = Runtime::new()
        .map_err(|e| IdsError::Data(format!("Failed to create async runtime: {e}")))?;
    
    // Check if path is directory or file
    if path.is_dir() {
        runtime.block_on(async {
            crate::data::io::parquet::load_parquet_directory(path, None, None).await
        })
    } else {
        // Use the ParquetReader to read a single file
        let mut reader = crate::data::io::parquet::ParquetReader::new(path);
        runtime.block_on(async {
            reader.read_async().await
        })
    }
}

/// Calculate balance metrics between case and control groups
pub fn calculate_balance(
    case_records: &[RecordBatch],
    control_records: &[RecordBatch],
) -> Result<BalanceReport> {
    // Ensure that we have data to work with
    if case_records.is_empty() || control_records.is_empty() {
        return Err(IdsError::Validation("No records found in case or control group".to_string()));
    }
    
    // Get the schema from the first batch
    let case_schema = case_records[0].schema();
    let control_schema = control_records[0].schema();
    
    // Calculate balance for each column that appears in both schemas
    let mut metrics = Vec::new();
    
    // Get all the columns from the case schema
    for field in case_schema.fields() {
        let column_name = field.name();
        
        // Skip if the column doesn't exist in control schema
        if control_schema.field_with_name(column_name).is_err() {
            continue;
        }
        
        // Handle the column based on its data type
        match field.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float32 | DataType::Float64 => {
                if let Ok(metric) = calculate_numeric_balance(case_records, control_records, column_name) {
                    metrics.push(metric);
                }
            },
            DataType::Utf8 => {
                if let Ok(metric) = calculate_categorical_balance(case_records, control_records, column_name) {
                    metrics.push(metric);
                }
            },
            // Skip other types for now
            _ => continue,
        }
    }
    
    // Calculate summary statistics
    let summary = calculate_summary_statistics(&metrics);
    
    Ok(BalanceReport { metrics, summary })
}

/// Calculate balance for a numeric column
fn calculate_numeric_balance(
    case_records: &[RecordBatch],
    control_records: &[RecordBatch],
    column_name: &str,
) -> Result<BalanceMetric> {
    // Extract numeric values from case records
    let mut case_values = Vec::new();
    for batch in case_records {
        if let Ok(col_idx) = batch.schema().index_of(column_name) {
            let array = batch.column(col_idx);
            
            // Handle different numeric types
            if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        case_values.push(f64::from(int_array.value(i)));
                    }
                }
            } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        case_values.push(float_array.value(i));
                    }
                }
            }
        }
    }
    
    // Extract numeric values from control records
    let mut control_values = Vec::new();
    for batch in control_records {
        if let Ok(col_idx) = batch.schema().index_of(column_name) {
            let array = batch.column(col_idx);
            
            // Handle different numeric types
            if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        control_values.push(f64::from(int_array.value(i)));
                    }
                }
            } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        control_values.push(float_array.value(i));
                    }
                }
            }
        }
    }
    
    // Skip if too few values
    if case_values.len() < 5 || control_values.len() < 5 {
        return Err(IdsError::Validation(format!("Too few values for numeric column {column_name}")));
    }
    
    // Calculate statistics
    let case_mean = mean(&case_values);
    let control_mean = mean(&control_values);
    let case_std = std_dev(&case_values, case_mean);
    let control_std = std_dev(&control_values, control_mean);
    
    // Calculate standardized difference
    let pooled_std = (control_std.mul_add(control_std, case_std.powi(2)) / 2.0).sqrt();
    let std_diff = if pooled_std > 0.0 {
        (case_mean - control_mean) / pooled_std
    } else {
        0.0
    };
    
    Ok(BalanceMetric {
        name: column_name.to_string(),
        standardized_difference: std_diff,
        case_mean,
        control_mean,
        case_std,
        control_std,
        categorical: false,
    })
}

/// Calculate balance for a categorical column
fn calculate_categorical_balance(
    case_records: &[RecordBatch],
    control_records: &[RecordBatch],
    column_name: &str,
) -> Result<BalanceMetric> {
    // Count frequencies for each category in cases
    let mut case_categories = HashMap::new();
    let mut case_total = 0;
    
    for batch in case_records {
        if let Ok(col_idx) = batch.schema().index_of(column_name) {
            let array = batch.column(col_idx);
            
            if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..str_array.len() {
                    if !str_array.is_null(i) {
                        let category = str_array.value(i);
                        *case_categories.entry(category.to_string()).or_insert(0) += 1;
                        case_total += 1;
                    }
                }
            }
        }
    }
    
    // Count frequencies for each category in controls
    let mut control_categories = HashMap::new();
    let mut control_total = 0;
    
    for batch in control_records {
        if let Ok(col_idx) = batch.schema().index_of(column_name) {
            let array = batch.column(col_idx);
            
            if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..str_array.len() {
                    if !str_array.is_null(i) {
                        let category = str_array.value(i);
                        *control_categories.entry(category.to_string()).or_insert(0) += 1;
                        control_total += 1;
                    }
                }
            }
        }
    }
    
    // Skip if too few values
    if case_total < 5 || control_total < 5 {
        return Err(IdsError::Validation(format!("Too few values for categorical column {column_name}")));
    }
    
    // For categorical variables, we'll use the most common category as a binary indicator
    // Find the most common category
    let most_common_category = case_categories
        .iter()
        .max_by_key(|&(_, count)| *count)
        .map(|(category, _)| category.clone())
        .unwrap_or_default();
    
    // Calculate proportion with most common category
    let case_count = case_categories.get(&most_common_category).unwrap_or(&0);
    let case_proportion = f64::from(*case_count) / f64::from(case_total);
    
    let control_count = control_categories.get(&most_common_category).unwrap_or(&0);
    let control_proportion = f64::from(*control_count) / f64::from(control_total);
    
    // For proportions, using special formula for standardized difference
    let std_diff = (case_proportion - control_proportion) / 
        (case_proportion.mul_add(1.0 - case_proportion, control_proportion * (1.0 - control_proportion)) / 2.0).sqrt();
    
    Ok(BalanceMetric {
        name: format!("{column_name}_category_{most_common_category}"),
        standardized_difference: std_diff,
        case_mean: case_proportion,
        control_mean: control_proportion,
        case_std: (case_proportion * (1.0 - case_proportion)).sqrt(), // Binomial SD
        control_std: (control_proportion * (1.0 - control_proportion)).sqrt(), // Binomial SD
        categorical: true,
    })
}

/// Calculate summary statistics for a set of balance metrics
fn calculate_summary_statistics(metrics: &[BalanceMetric]) -> BalanceSummary {
    let mut imbalanced = 0;
    let mut max_std_diff = 0.0;
    let mut sum_abs_std_diff = 0.0;
    
    for metric in metrics {
        let abs_std_diff = metric.standardized_difference.abs();
        
        if abs_std_diff > 0.1 {
            imbalanced += 1;
        }
        
        if abs_std_diff > max_std_diff {
            max_std_diff = abs_std_diff;
        }
        
        sum_abs_std_diff += abs_std_diff;
    }
    
    let mean_abs_std_diff = if metrics.is_empty() {
        0.0
    } else {
        sum_abs_std_diff / metrics.len() as f64
    };
    
    BalanceSummary {
        imbalanced_covariates: imbalanced,
        max_standardized_difference: max_std_diff,
        mean_absolute_standardized_difference: mean_abs_std_diff,
        total_covariates: metrics.len(),
    }
}

/// Generate a balance report in CSV format
/// 
/// This function has been moved to `utils::reports::generate_balance_report`
/// and is re-exported here for backward compatibility.
pub fn generate_balance_report(report_path: &str, report: &BalanceReport) -> Result<()> {
    crate::utils::reports::generate_balance_report(report_path, report)
}