//! Filtering capabilities for Parquet files
//!
//! This module provides a flexible expression-based filtering system
//! for Parquet files, allowing you to filter rows based on column values.

use anyhow::Context;
use std::collections::HashSet;
use std::path::Path;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::filter as arrow_filter;

use arrow::record_batch::RecordBatch;

use crate::error::{ParquetReaderError, Result};
use crate::utils;

/// Represents a filter expression for querying Parquet data
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column equals a literal value
    Eq(String, LiteralValue),

    /// Column not equals a literal value
    NotEq(String, LiteralValue),

    /// Column is greater than a literal value
    Gt(String, LiteralValue),

    /// Column is greater than or equal to a literal value
    GtEq(String, LiteralValue),

    /// Column is less than a literal value
    Lt(String, LiteralValue),

    /// Column is less than or equal to a literal value
    LtEq(String, LiteralValue),

    /// Column is in a set of values
    In(String, Vec<LiteralValue>),

    /// Column is not in a set of values
    NotIn(String, Vec<LiteralValue>),

    /// Column is null
    IsNull(String),

    /// Column is not null
    IsNotNull(String),

    /// Column value contains a substring
    Contains(String, String),

    /// Column value starts with a prefix
    StartsWith(String, String),

    /// Column value ends with a suffix
    EndsWith(String, String),

    /// Logical AND of expressions
    And(Vec<Expr>),

    /// Logical OR of expressions
    Or(Vec<Expr>),

    /// Logical NOT of an expression
    Not(Box<Expr>),

    /// Always evaluates to true
    AlwaysTrue,

    /// Always evaluates to false
    AlwaysFalse,
}

/// Represents a literal value that can be used in filter expressions
#[derive(Debug, Clone)]
pub enum LiteralValue {
    /// Boolean value
    Boolean(bool),

    /// Integer value
    Int(i64),

    /// Floating point value
    Float(f64),

    /// String value
    String(String),

    /// Date value (days since epoch)
    Date(i32),

    /// Timestamp value (milliseconds since epoch)
    Timestamp(i64),

    /// Null value
    Null,
}

impl Expr {
    /// Returns a set of all column names required by this expression
    #[must_use]
    pub fn required_columns(&self) -> HashSet<String> {
        let mut columns = HashSet::new();
        self.collect_required_columns(&mut columns);
        columns
    }

    /// Helper method to collect column names
    fn collect_required_columns(&self, columns: &mut HashSet<String>) {
        match self {
            Self::Eq(col, _)
            | Self::NotEq(col, _)
            | Self::Gt(col, _)
            | Self::GtEq(col, _)
            | Self::Lt(col, _)
            | Self::LtEq(col, _)
            | Self::In(col, _)
            | Self::NotIn(col, _)
            | Self::IsNull(col)
            | Self::IsNotNull(col)
            | Self::Contains(col, _)
            | Self::StartsWith(col, _)
            | Self::EndsWith(col, _) => {
                columns.insert(col.clone());
            }
            Self::And(exprs) | Self::Or(exprs) => {
                for expr in exprs {
                    expr.collect_required_columns(columns);
                }
            }
            Self::Not(expr) => {
                expr.collect_required_columns(columns);
            }
            Self::AlwaysTrue | Self::AlwaysFalse => {}
        }
    }
}

/// Evaluates a filter expression against a record batch
///
/// # Arguments
/// * `batch` - The record batch to filter
/// * `expr` - The filter expression to apply
///
/// # Returns
/// A boolean array indicating which rows match the filter
///
/// # Errors
/// Returns an error if expression evaluation fails
pub fn evaluate_expr(batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
    match expr {
        Expr::AlwaysTrue => {
            // Create a boolean array with all true values
            let values = vec![true; batch.num_rows()];
            Ok(BooleanArray::from(values))
        }

        Expr::AlwaysFalse => {
            // Create a boolean array with all false values
            let values = vec![false; batch.num_rows()];
            Ok(BooleanArray::from(values))
        }

        Expr::And(exprs) => evaluate_and_expression(batch, exprs),

        Expr::Or(exprs) => evaluate_or_expression(batch, exprs),

        Expr::Not(expr) => evaluate_not_expression(batch, expr),

        Expr::Eq(col_name, literal_value) => evaluate_eq_expression(batch, col_name, literal_value),

        Expr::Gt(col_name, literal_value) => evaluate_gt_expression(batch, col_name, literal_value),

        // Other expression types would be implemented similarly
        _ => Err(anyhow::anyhow!("Unsupported filter expression: {expr:?}")),
    }
}

/// Evaluates a logical AND expression
fn evaluate_and_expression(batch: &RecordBatch, exprs: &[Expr]) -> Result<BooleanArray> {
    use arrow::compute::and;

    if exprs.is_empty() {
        return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
    }

    // Evaluate the first expression
    let mut result = evaluate_expr(batch, &exprs[0])?;

    // Apply AND with each subsequent expression using vectorized operations
    for expr in &exprs[1..] {
        let mask = evaluate_expr(batch, expr)?;

        // Use Arrow's vectorized 'and' function instead of row-by-row
        let result_ref = and(&result, &mask)
            .context("Failed to apply AND operation to filter arrays")?;

        result = result_ref
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to downcast boolean array"))?
            .clone();
    }

    Ok(result)
}

/// Evaluates a logical OR expression
fn evaluate_or_expression(batch: &RecordBatch, exprs: &[Expr]) -> Result<BooleanArray> {
    use arrow::compute::or;

    if exprs.is_empty() {
        return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
    }

    // Evaluate the first expression
    let mut result = evaluate_expr(batch, &exprs[0])?;

    // Apply OR with each subsequent expression using vectorized operations
    for expr in &exprs[1..] {
        let mask = evaluate_expr(batch, expr)?;

        // Use Arrow's vectorized 'or' function instead of row-by-row
        let result_ref = or(&result, &mask)
            .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

        result = result_ref
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to downcast boolean array"))?
            .clone();
    }

    Ok(result)
}

/// Evaluates a logical NOT expression
fn evaluate_not_expression(batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
    use arrow::compute::not;

    let mask = evaluate_expr(batch, expr)?;

    // Use Arrow's vectorized 'not' function instead of row-by-row
    let result = not(&mask).map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

    Ok(result
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ParquetReaderError::FilterError("Failed to downcast boolean array".to_string())
        })?
        .clone())
}

/// Evaluates an equality comparison expression
fn evaluate_eq_expression(batch: &RecordBatch, col_name: &str, literal_value: &LiteralValue) -> Result<BooleanArray> {
    // Get the column
    let col_idx = batch.schema().index_of(col_name).map_err(|_| {
        ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
    })?;
    let column = batch.column(col_idx);

    // Create a boolean mask based on equality comparison using vectorized operations
    match literal_value {
        LiteralValue::String(s) => evaluate_string_eq(column, col_name, s),
        LiteralValue::Int(n) => evaluate_int_eq(column, col_name, *n),
        _ => Err(anyhow::anyhow!(
            "Unsupported literal type for equality comparison: {literal_value:?}"
        )),
    }
}

/// Evaluates equality comparison for string types
fn evaluate_string_eq(column: &arrow::array::ArrayRef, col_name: &str, s: &str) -> Result<BooleanArray> {
    use arrow::compute::kernels::cmp::eq;
    
    if let Some(str_array) = column.as_any().downcast_ref::<arrow::array::StringArray>() {
        // Create a constant array of comparison values
        let literal_array = arrow::array::StringArray::from(vec![s; str_array.len()]);

        // Use Arrow's vectorized eq function
        let result = eq(str_array, &literal_array)
            .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

        Ok(result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ParquetReaderError::FilterError("Failed to downcast boolean array".to_string())
            })?
            .clone())
    } else {
        Err(anyhow::anyhow!("Column {col_name} is not a string array"))
    }
}

/// Evaluates equality comparison for integer types
fn evaluate_int_eq(column: &arrow::array::ArrayRef, col_name: &str, n: i64) -> Result<BooleanArray> {
    use arrow::compute::kernels::cmp::eq;
    
    if let Some(int_array) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        // Convert i64 to i32 safely
        if let Ok(n_i32) = i32::try_from(n) {
            // Create a constant array of comparison values
            let literal_array = arrow::array::Int32Array::from(vec![n_i32; int_array.len()]);

            // Use Arrow's vectorized eq function
            let result = eq(int_array, &literal_array)
                .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

            Ok(result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ParquetReaderError::FilterError("Failed to downcast boolean array".to_string())
                })?
                .clone())
        } else {
            // If the i64 value doesn't fit in i32, the equality will always be false
            Ok(arrow::array::BooleanArray::from(vec![false; int_array.len()]))
        }
    } else if let Some(int_array) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
        // Create a constant array of comparison values
        let literal_array = arrow::array::Int64Array::from(vec![n; int_array.len()]);

        // Use Arrow's vectorized eq function
        let result = eq(int_array, &literal_array)
            .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

        Ok(result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ParquetReaderError::FilterError("Failed to downcast boolean array".to_string())
            })?
            .clone())
    } else {
        Err(anyhow::anyhow!("Column {col_name} is not an integer array"))
    }
}

/// Evaluates a greater than comparison expression
fn evaluate_gt_expression(batch: &RecordBatch, col_name: &str, literal_value: &LiteralValue) -> Result<BooleanArray> {
    // Get the column
    let col_idx = batch.schema().index_of(col_name).map_err(|_| {
        ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
    })?;
    let column = batch.column(col_idx);

    // Create a boolean mask based on greater than comparison using vectorized operations
    match literal_value {
        LiteralValue::Int(n) => evaluate_int_gt(column, col_name, *n),
        _ => Err(anyhow::anyhow!(
            "Unsupported literal type for greater than comparison: {literal_value:?}"
        )),
    }
}

/// Evaluates greater than comparison for integer types
fn evaluate_int_gt(column: &arrow::array::ArrayRef, col_name: &str, n: i64) -> Result<BooleanArray> {
    use arrow::compute::kernels::cmp::gt;
    
    if let Some(int_array) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        // Convert i64 to i32 safely
        if let Ok(n_i32) = i32::try_from(n) {
            // Create a constant array of comparison values
            let literal_array = arrow::array::Int32Array::from(vec![n_i32; int_array.len()]);

            // Use Arrow's vectorized gt function
            let result = gt(int_array, &literal_array)
                .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

            Ok(result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ParquetReaderError::FilterError("Failed to downcast boolean array".to_string())
                })?
                .clone())
        } else {
            // If i64 value is too large for i32, for "a > b" the result depends on the sign
            // If n is positive, nothing can be greater, so all false
            // If n is negative, all values would be greater, so all true
            let result = if n > 0 {
                vec![false; int_array.len()]
            } else {
                vec![true; int_array.len()]
            };
            Ok(arrow::array::BooleanArray::from(result))
        }
    } else if let Some(int_array) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
        // Create a constant array of comparison values
        let literal_array = arrow::array::Int64Array::from(vec![n; int_array.len()]);

        // Use Arrow's vectorized gt function
        let result = gt(int_array, &literal_array)
            .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

        Ok(result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ParquetReaderError::FilterError("Failed to downcast boolean array".to_string())
            })?
            .clone())
    } else {
        Err(anyhow::anyhow!("Column {col_name} is not an integer array"))
    }
}

/// Filters a record batch based on a boolean mask
///
/// # Arguments
/// * `batch` - The record batch to filter
/// * `mask` - The boolean mask indicating which rows to keep
///
/// # Returns
/// A new record batch with only rows where mask is true
///
/// # Errors
/// Returns an error if filtering fails
pub fn filter_record_batch(batch: &RecordBatch, mask: &BooleanArray) -> Result<RecordBatch> {
    // Validate with clear error message
    if batch.num_rows() != mask.len() {
        return Err(anyhow::anyhow!(
            "Mask length ({}) doesn't match batch row count ({})",
            mask.len(),
            batch.num_rows()
        ));
    }

    // Apply the filter to all columns with specific error context
    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow_filter(col, mask))
        .collect::<arrow::error::Result<_>>()
        .map_err(|e| {
            ParquetReaderError::arrow_error_with_source(
                "Failed to apply boolean filter to columns",
                e,
            )
        })?;

    // Create a new record batch with filtered data
    RecordBatch::try_new(batch.schema(), filtered_columns)
        .map_err(|e| anyhow::anyhow!("Failed to create filtered record batch. Error: {}", e))
}

/// Reads a Parquet file with filtering
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `expr` - Filter expression to apply
/// * `columns` - Optional columns to include in the result
///
/// # Returns
/// A vector of filtered record batches
///
/// # Errors
/// Returns an error if file reading or filtering fails
pub fn read_parquet_with_filter(
    path: &Path,
    expr: &Expr,
    columns: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    // Validate path
    if !path.exists() {
        return Err(anyhow::anyhow!("File not found"));
    }

    // Get all columns required by the filter expression - used for projection below
    let mut all_required_columns = expr.required_columns();

    // Add any additional columns requested for projection
    if let Some(cols) = columns {
        for col in cols {
            all_required_columns.insert(col.clone());
        }
    }

    // Read the file with schema projection containing required columns
    let batches = utils::read_parquet::<std::collections::hash_map::RandomState>(path, None, None)
        .with_context(|| format!("Failed to read Parquet file for filtering (path: {})", path.display()))?;

    // Filter each batch
    let mut filtered_batches = Vec::new();
    for (i, batch) in batches.iter().enumerate() {
        // Evaluate the filter expression
        let mask = evaluate_expr(batch, expr)
            .map_err(|e| e.context(format!("Failed to evaluate filter on batch {i}")))?;

        // Apply the filter to the batch
        let filtered_batch = filter_record_batch(batch, &mask)
            .map_err(|e| e.context(format!("Failed to apply filter to batch {i}")))?;

        // Only add non-empty batches
        if filtered_batch.num_rows() > 0 {
            filtered_batches.push(filtered_batch);
        }
    }

    if filtered_batches.is_empty() {
        log::info!(
            "No records matched the filter criteria in file: {}",
            path.display()
        );
    } else {
        log::debug!(
            "Applied filter to file {}: filtered {} batches with {} total rows",
            path.display(),
            filtered_batches.len(),
            filtered_batches.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>()
        );
    }

    Ok(filtered_batches)
}

/// Helper function to create equality filter for PNRs
///
/// # Arguments
/// * `pnrs` - Set of PNRs to match against
///
/// # Returns
/// An expression that matches records where PNR is in the provided set
#[must_use]
pub fn create_pnr_filter<S: ::std::hash::BuildHasher>(pnrs: &HashSet<String, S>) -> Expr {
    let values = pnrs
        .iter()
        .map(|s| LiteralValue::String(s.clone()))
        .collect();

    Expr::In("PNR".to_string(), values)
}
