//! Balance assessment for case-control matching
//!
//! This module provides functions and structures for assessing the balance
//! of covariates between matched case and control groups.

use crate::error::{ParquetReaderError, Result};
use arrow::array::{Array, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use log::{info, warn};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

/// Metric for balance between cases and controls for a single covariate
#[derive(Debug, Clone)]
pub struct BalanceMetric {
    /// Name of the covariate (column)
    pub name: String,

    /// Standardized difference between case and control groups
    pub standardized_difference: f64,

    /// Mean (or proportion) for cases
    pub case_mean: f64,

    /// Mean (or proportion) for controls
    pub control_mean: f64,

    /// Standard deviation for cases
    pub case_std: f64,

    /// Standard deviation for controls
    pub control_std: f64,

    /// Whether the covariate is categorical
    pub categorical: bool,
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

/// Report on balance between cases and controls
#[derive(Debug, Clone)]
pub struct BalanceReport {
    /// Balance metrics for each covariate
    pub metrics: Vec<BalanceMetric>,

    /// Summary statistics
    pub summary: BalanceSummary,
}

impl BalanceReport {
    /// Generate a string representation of the report
    #[must_use] pub fn to_string(&self) -> String {
        let mut output = String::new();

        // Add summary information
        output.push_str(&format!(
            "Balance Summary:\n\
             - Total covariates: {}\n\
             - Imbalanced covariates (std diff > 0.1): {} ({:.1}%)\n\
             - Maximum standardized difference: {:.4}\n\
             - Mean absolute standardized difference: {:.4}\n\n",
            self.summary.total_covariates,
            self.summary.imbalanced_covariates,
            if self.summary.total_covariates > 0 {
                100.0 * self.summary.imbalanced_covariates as f64
                    / self.summary.total_covariates as f64
            } else {
                0.0
            },
            self.summary.max_standardized_difference,
            self.summary.mean_absolute_standardized_difference
        ));

        // Add table header
        output.push_str(
            "Covariate                      | Type      | Case Mean | Control Mean | Case SD  | Control SD | Std Diff\n\
             ------------------------------|-----------|-----------|--------------|----------|------------|----------\n"
        );

        // Sort metrics by absolute standardized difference (descending)
        let mut sorted_metrics = self.metrics.clone();
        sorted_metrics.sort_by(|a, b| {
            b.standardized_difference
                .abs()
                .partial_cmp(&a.standardized_difference.abs())
                .unwrap()
        });

        // Add each metric
        for metric in &sorted_metrics {
            let covariate_type = if metric.categorical {
                "Categorical"
            } else {
                "Continuous"
            };

            output.push_str(&format!(
                "{:<30} | {:<9} | {:>9.4} | {:>12.4} | {:>8.4} | {:>10.4} | {:>8.4}\n",
                truncate_string(&metric.name, 30),
                covariate_type,
                metric.case_mean,
                metric.control_mean,
                metric.case_std,
                metric.control_std,
                metric.standardized_difference
            ));
        }

        output
    }

    /// Write the report to a CSV file
    pub fn write_to_csv(&self, file_path: &str) -> Result<()> {
        let mut file = File::create(file_path).map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to create balance report file: {e}"))
        })?;

        // Write header
        writeln!(
            file,
            "Covariate,Type,Case Mean,Control Mean,Case SD,Control SD,Std Diff"
        )
        .map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
        })?;

        // Sort metrics by absolute standardized difference (descending)
        let mut sorted_metrics = self.metrics.clone();
        sorted_metrics.sort_by(|a, b| {
            b.standardized_difference
                .abs()
                .partial_cmp(&a.standardized_difference.abs())
                .unwrap()
        });

        // Write each metric
        for metric in &sorted_metrics {
            let covariate_type = if metric.categorical {
                "Categorical"
            } else {
                "Continuous"
            };

            writeln!(
                file,
                "{},{},{:.6},{:.6},{:.6},{:.6},{:.6}",
                escape_csv(&metric.name),
                covariate_type,
                metric.case_mean,
                metric.control_mean,
                metric.case_std,
                metric.control_std,
                metric.standardized_difference
            )
            .map_err(|e| {
                ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
            })?;
        }

        // Write summary
        writeln!(file).map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
        })?;
        writeln!(file, "Summary Statistics,,,,,").map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
        })?;
        writeln!(
            file,
            "Total covariates,{},,,,,",
            self.summary.total_covariates
        )
        .map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
        })?;
        writeln!(
            file,
            "Imbalanced covariates (std diff > 0.1),{},,,,,",
            self.summary.imbalanced_covariates
        )
        .map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
        })?;
        writeln!(
            file,
            "Maximum standardized difference,{:.6},,,,,",
            self.summary.max_standardized_difference
        )
        .map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
        })?;
        writeln!(
            file,
            "Mean absolute standardized difference,{:.6},,,,,",
            self.summary.mean_absolute_standardized_difference
        )
        .map_err(|e| {
            ParquetReaderError::IoError(format!("Failed to write to balance report file: {e}"))
        })?;

        Ok(())
    }
}

/// Calculator for balance metrics between case and control groups
pub struct BalanceCalculator {
    /// Columns to exclude from balance assessment
    exclude_columns: Vec<String>,

    /// Minimum required observations for calculating balance
    min_observations: usize,

    /// Threshold for marking a covariate as imbalanced
    imbalance_threshold: f64,
}

impl Default for BalanceCalculator {
    fn default() -> Self {
        Self {
            exclude_columns: vec!["PNR".to_string()], // Default to excluding identifier columns
            min_observations: 5,                      // At least 5 observations required
            imbalance_threshold: 0.1,                 // Standardized difference > 0.1 is imbalanced
        }
    }
}

impl BalanceCalculator {
    /// Create a new balance calculator with default settings
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set columns to exclude from balance assessment
    #[must_use]
    pub fn with_exclude_columns(mut self, columns: Vec<String>) -> Self {
        self.exclude_columns = columns;
        self
    }

    /// Set minimum required observations for calculating balance
    #[must_use]
    pub const fn with_min_observations(mut self, min_observations: usize) -> Self {
        self.min_observations = min_observations;
        self
    }

    /// Set threshold for marking a covariate as imbalanced
    #[must_use]
    pub const fn with_imbalance_threshold(mut self, threshold: f64) -> Self {
        self.imbalance_threshold = threshold;
        self
    }

    /// Calculate balance metrics between case and control groups
    ///
    /// # Arguments
    ///
    /// * `case_batch` - `RecordBatch` containing case records
    /// * `control_batch` - `RecordBatch` containing control records
    ///
    /// # Returns
    ///
    /// Result containing a `BalanceReport` with metrics and summary statistics
    pub fn calculate_balance(
        &self,
        case_batch: &RecordBatch,
        control_batch: &RecordBatch,
    ) -> Result<BalanceReport> {
        // Ensure that we have data to work with
        if case_batch.num_rows() == 0 || control_batch.num_rows() == 0 {
            return Err(ParquetReaderError::ValidationError(
                "No records found in case or control batch".to_string(),
            )
            .into());
        }

        let case_schema = case_batch.schema();
        let control_schema = control_batch.schema();

        // Calculate balance for each column that appears in both schemas
        let mut metrics = Vec::new();

        // Get all the columns from the case schema
        for field in case_schema.fields() {
            let column_name = field.name();

            // Skip excluded columns
            if self
                .exclude_columns
                .iter()
                .any(|excluded| excluded == column_name)
            {
                continue;
            }

            // Skip if the column doesn't exist in control schema
            if control_schema.field_with_name(column_name).is_err() {
                continue;
            }

            // Handle the column based on its data type
            match field.data_type() {
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64 => {
                    match self.calculate_numeric_balance(case_batch, control_batch, column_name) {
                        Ok(metric) => metrics.push(metric),
                        Err(e) => warn!(
                            "Failed to calculate balance for numeric column {column_name}: {e}"
                        ),
                    }
                }
                DataType::Utf8 => {
                    match self.calculate_categorical_balance(case_batch, control_batch, column_name)
                    {
                        Ok(metric) => metrics.push(metric),
                        Err(e) => warn!(
                            "Failed to calculate balance for categorical column {column_name}: {e}"
                        ),
                    }
                }
                DataType::Boolean => {
                    match self.calculate_boolean_balance(case_batch, control_batch, column_name) {
                        Ok(metric) => metrics.push(metric),
                        Err(e) => warn!(
                            "Failed to calculate balance for boolean column {column_name}: {e}"
                        ),
                    }
                }
                DataType::Date32 | DataType::Date64 => {
                    warn!(
                        "Date column {column_name} not supported yet for balance calculation"
                    );
                }
                _ => {
                    // Skip other types
                    warn!(
                        "Column {} has unsupported data type for balance calculation: {:?}",
                        column_name,
                        field.data_type()
                    );
                }
            }
        }

        // Calculate summary statistics
        let summary = self.calculate_summary_statistics(&metrics);

        info!(
            "Balance assessment complete: {} of {} covariates are imbalanced (stdiff > {})",
            summary.imbalanced_covariates, summary.total_covariates, self.imbalance_threshold
        );

        Ok(BalanceReport { metrics, summary })
    }

    /// Calculate balance for a numeric column
    fn calculate_numeric_balance(
        &self,
        case_batch: &RecordBatch,
        control_batch: &RecordBatch,
        column_name: &str,
    ) -> Result<BalanceMetric> {
        // Extract numeric values from case batch
        let mut case_values = Vec::new();
        let case_col_idx = case_batch.schema().index_of(column_name).map_err(|_| {
            ParquetReaderError::ValidationError(format!(
                "Column {column_name} not found in case batch"
            ))
        })?;

        let case_array = case_batch.column(case_col_idx);
        extract_numeric_values(case_array, &mut case_values);

        // Extract numeric values from control batch
        let mut control_values = Vec::new();
        let control_col_idx = control_batch.schema().index_of(column_name).map_err(|_| {
            ParquetReaderError::ValidationError(format!(
                "Column {column_name} not found in control batch"
            ))
        })?;

        let control_array = control_batch.column(control_col_idx);
        extract_numeric_values(control_array, &mut control_values);

        // Skip if too few values
        if case_values.len() < self.min_observations || control_values.len() < self.min_observations
        {
            return Err(ParquetReaderError::ValidationError(format!(
                "Too few non-missing values for numeric column {column_name} (case: {}, control: {})",
                case_values.len(),
                control_values.len()
            )).into());
        }

        // Calculate statistics
        let case_mean = calculate_mean(&case_values);
        let control_mean = calculate_mean(&control_values);
        let case_std = calculate_std_dev(&case_values, case_mean);
        let control_std = calculate_std_dev(&control_values, control_mean);

        // Calculate standardized difference
        let standardized_difference =
            calculate_standardized_difference(case_mean, control_mean, case_std, control_std);

        Ok(BalanceMetric {
            name: column_name.to_string(),
            standardized_difference,
            case_mean,
            control_mean,
            case_std,
            control_std,
            categorical: false,
        })
    }

    /// Calculate balance for a categorical column
    fn calculate_categorical_balance(
        &self,
        case_batch: &RecordBatch,
        control_batch: &RecordBatch,
        column_name: &str,
    ) -> Result<BalanceMetric> {
        // Count frequencies for each category in cases
        let mut case_categories = HashMap::new();
        let mut case_total = 0;

        let case_col_idx = case_batch.schema().index_of(column_name).map_err(|_| {
            ParquetReaderError::ValidationError(format!(
                "Column {column_name} not found in case batch"
            ))
        })?;

        let case_array = case_batch.column(case_col_idx);
        if let Some(str_array) = case_array.as_any().downcast_ref::<StringArray>() {
            for i in 0..str_array.len() {
                if !str_array.is_null(i) {
                    let category = str_array.value(i);
                    *case_categories.entry(category.to_string()).or_insert(0) += 1;
                    case_total += 1;
                }
            }
        } else {
            return Err(ParquetReaderError::ValidationError(format!(
                "Column {column_name} is not a string array"
            ))
            .into());
        }

        // Count frequencies for each category in controls
        let mut control_categories = HashMap::new();
        let mut control_total = 0;

        let control_col_idx = control_batch.schema().index_of(column_name).map_err(|_| {
            ParquetReaderError::ValidationError(format!(
                "Column {column_name} not found in control batch"
            ))
        })?;

        let control_array = control_batch.column(control_col_idx);
        if let Some(str_array) = control_array.as_any().downcast_ref::<StringArray>() {
            for i in 0..str_array.len() {
                if !str_array.is_null(i) {
                    let category = str_array.value(i);
                    *control_categories.entry(category.to_string()).or_insert(0) += 1;
                    control_total += 1;
                }
            }
        } else {
            return Err(ParquetReaderError::ValidationError(format!(
                "Column {column_name} is not a string array"
            ))
            .into());
        }

        // Skip if too few values
        if case_total < self.min_observations || control_total < self.min_observations {
            return Err(ParquetReaderError::ValidationError(format!(
                "Too few non-missing values for categorical column {column_name} (case: {case_total}, control: {control_total})"
            )).into());
        }

        // Find the most common category in the combined dataset for balance assessment
        let mut combined_categories = HashMap::new();
        for (category, count) in &case_categories {
            *combined_categories.entry(category.clone()).or_insert(0) += *count;
        }
        for (category, count) in &control_categories {
            *combined_categories.entry(category.clone()).or_insert(0) += *count;
        }

        let most_common_category = combined_categories
            .iter()
            .max_by_key(|&(_, count)| *count)
            .map(|(category, _)| category.clone())
            .unwrap_or_default();

        // Calculate proportion with most common category
        let case_count = case_categories.get(&most_common_category).unwrap_or(&0);
        let case_proportion = f64::from(*case_count) / case_total as f64;

        let control_count = control_categories.get(&most_common_category).unwrap_or(&0);
        let control_proportion = f64::from(*control_count) / control_total as f64;

        // For proportions, using special formula for standardized difference
        let case_std = (case_proportion * (1.0 - case_proportion)).sqrt();
        let control_std = (control_proportion * (1.0 - control_proportion)).sqrt();

        let standardized_difference = if case_std == 0.0 && control_std == 0.0 {
            0.0 // Both are constants, no difference
        } else {
            let pooled_std = (control_std.mul_add(control_std, case_std.powi(2)) / 2.0).sqrt();
            if pooled_std > 0.0 {
                (case_proportion - control_proportion) / pooled_std
            } else {
                0.0
            }
        };

        Ok(BalanceMetric {
            name: format!("{column_name}_{most_common_category}"),
            standardized_difference,
            case_mean: case_proportion,
            control_mean: control_proportion,
            case_std,
            control_std,
            categorical: true,
        })
    }

    /// Calculate balance for a boolean column
    fn calculate_boolean_balance(
        &self,
        case_batch: &RecordBatch,
        control_batch: &RecordBatch,
        column_name: &str,
    ) -> Result<BalanceMetric> {
        // Count true values in cases
        let mut case_true_count = 0;
        let mut case_total = 0;

        let case_col_idx = case_batch.schema().index_of(column_name).map_err(|_| {
            ParquetReaderError::ValidationError(format!(
                "Column {column_name} not found in case batch"
            ))
        })?;

        let case_array = case_batch.column(case_col_idx);
        if let Some(bool_array) = case_array.as_any().downcast_ref::<BooleanArray>() {
            for i in 0..bool_array.len() {
                if !bool_array.is_null(i) {
                    if bool_array.value(i) {
                        case_true_count += 1;
                    }
                    case_total += 1;
                }
            }
        } else {
            return Err(ParquetReaderError::ValidationError(format!(
                "Column {column_name} is not a boolean array"
            ))
            .into());
        }

        // Count true values in controls
        let mut control_true_count = 0;
        let mut control_total = 0;

        let control_col_idx = control_batch.schema().index_of(column_name).map_err(|_| {
            ParquetReaderError::ValidationError(format!(
                "Column {column_name} not found in control batch"
            ))
        })?;

        let control_array = control_batch.column(control_col_idx);
        if let Some(bool_array) = control_array.as_any().downcast_ref::<BooleanArray>() {
            for i in 0..bool_array.len() {
                if !bool_array.is_null(i) {
                    if bool_array.value(i) {
                        control_true_count += 1;
                    }
                    control_total += 1;
                }
            }
        } else {
            return Err(ParquetReaderError::ValidationError(format!(
                "Column {column_name} is not a boolean array"
            ))
            .into());
        }

        // Skip if too few values
        if case_total < self.min_observations || control_total < self.min_observations {
            return Err(ParquetReaderError::ValidationError(format!(
                "Too few non-missing values for boolean column {column_name} (case: {case_total}, control: {control_total})"
            )).into());
        }

        // Calculate proportions
        let case_proportion = f64::from(case_true_count) / case_total as f64;
        let control_proportion = f64::from(control_true_count) / control_total as f64;

        // For proportions, using special formula for standardized difference
        let case_std = (case_proportion * (1.0 - case_proportion)).sqrt();
        let control_std = (control_proportion * (1.0 - control_proportion)).sqrt();

        let standardized_difference = if case_std == 0.0 && control_std == 0.0 {
            0.0 // Both are constants, no difference
        } else {
            let pooled_std = (control_std.mul_add(control_std, case_std.powi(2)) / 2.0).sqrt();
            if pooled_std > 0.0 {
                (case_proportion - control_proportion) / pooled_std
            } else {
                0.0
            }
        };

        Ok(BalanceMetric {
            name: format!("{column_name}_TRUE"),
            standardized_difference,
            case_mean: case_proportion,
            control_mean: control_proportion,
            case_std,
            control_std,
            categorical: true,
        })
    }

    /// Calculate summary statistics for a set of balance metrics
    fn calculate_summary_statistics(&self, metrics: &[BalanceMetric]) -> BalanceSummary {
        let mut imbalanced = 0;
        let mut max_std_diff = 0.0;
        let mut sum_abs_std_diff = 0.0;

        for metric in metrics {
            let abs_std_diff = metric.standardized_difference.abs();

            if abs_std_diff > self.imbalance_threshold {
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
}

/// Helper function to extract numeric values from an Arrow array
fn extract_numeric_values(array: &arrow::array::ArrayRef, values: &mut Vec<f64>) {
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Int32 => {
            if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        values.push(f64::from(int_array.value(i)));
                    }
                }
            }
        }
        DataType::Int64 => {
            if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        values.push(int_array.value(i) as f64);
                    }
                }
            }
        }
        DataType::Float32 => {
            if let Some(float_array) = array.as_any().downcast_ref::<Float32Array>() {
                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        let val = f64::from(float_array.value(i));
                        if val.is_finite() {
                            values.push(val);
                        }
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        let val = float_array.value(i);
                        if val.is_finite() {
                            values.push(val);
                        }
                    }
                }
            }
        }
        _ => {
            // Other numeric types would be handled here
        }
    }
}

/// Calculate the mean of a vector of values
fn calculate_mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let sum: f64 = values.iter().sum();
    sum / values.len() as f64
}

/// Calculate the standard deviation of a vector of values
fn calculate_std_dev(values: &[f64], mean: f64) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }

    let variance =
        values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;

    variance.sqrt()
}

/// Calculate the standardized difference between two groups
fn calculate_standardized_difference(mean1: f64, mean2: f64, std1: f64, std2: f64) -> f64 {
    if std1 == 0.0 && std2 == 0.0 {
        return 0.0; // Both are constants, no difference
    }

    let pooled_std = (std2.mul_add(std2, std1.powi(2)) / 2.0).sqrt();

    if pooled_std > 0.0 {
        (mean1 - mean2) / pooled_std
    } else {
        0.0
    }
}

/// Truncate a string to a maximum length
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[0..max_len - 3])
    }
}

/// Escape a string for CSV output
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
