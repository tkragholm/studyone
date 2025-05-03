//! Filtering capabilities for Parquet files
//!
//! This module provides a flexible expression-based filtering system
//! for Parquet files, allowing you to filter rows based on column values.

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

        Expr::And(exprs) => {
            if exprs.is_empty() {
                return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
            }

            // Evaluate the first expression
            let mut result = evaluate_expr(batch, &exprs[0])?;

            // Apply AND with each subsequent expression
            for expr in &exprs[1..] {
                let mask = evaluate_expr(batch, expr)?;

                // Apply logical AND
                result = apply_logical_and(&result, &mask)?;
            }

            Ok(result)
        }

        Expr::Or(exprs) => {
            if exprs.is_empty() {
                return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
            }

            // Evaluate the first expression
            let mut result = evaluate_expr(batch, &exprs[0])?;

            // Apply OR with each subsequent expression
            for expr in &exprs[1..] {
                let mask = evaluate_expr(batch, expr)?;

                // Apply logical OR
                result = apply_logical_or(&result, &mask)?;
            }

            Ok(result)
        }

        Expr::Not(expr) => {
            let mask = evaluate_expr(batch, expr)?;

            // Apply logical NOT
            let mut values = Vec::with_capacity(mask.len());
            for i in 0..mask.len() {
                if mask.is_null(i) {
                    values.push(false); // NULL becomes false when NOT applied
                } else {
                    values.push(!mask.value(i));
                }
            }

            Ok(BooleanArray::from(values))
        }

        Expr::Eq(col_name, literal_value) => {
            // Get the column
            let col_idx = batch.schema().index_of(col_name).map_err(|_| {
                ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
            })?;
            let column = batch.column(col_idx);

            // Create a boolean mask based on equality comparison
            match literal_value {
                LiteralValue::String(s) => {
                    if let Some(str_array) =
                        column.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        let mut values = Vec::with_capacity(str_array.len());
                        for i in 0..str_array.len() {
                            if str_array.is_null(i) {
                                values.push(false);
                            } else {
                                values.push(str_array.value(i) == s);
                            }
                        }
                        Ok(BooleanArray::from(values))
                    } else {
                        Err(ParquetReaderError::FilterError(format!(
                            "Column {col_name} is not a string array"
                        )))
                    }
                }
                LiteralValue::Int(n) => {
                    if let Some(int_array) =
                        column.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        let mut values = Vec::with_capacity(int_array.len());
                        for i in 0..int_array.len() {
                            if int_array.is_null(i) {
                                values.push(false);
                            } else {
                                values.push(int_array.value(i) as i64 == *n);
                            }
                        }
                        Ok(BooleanArray::from(values))
                    } else if let Some(int_array) =
                        column.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        let mut values = Vec::with_capacity(int_array.len());
                        for i in 0..int_array.len() {
                            if int_array.is_null(i) {
                                values.push(false);
                            } else {
                                values.push(int_array.value(i) == *n);
                            }
                        }
                        Ok(BooleanArray::from(values))
                    } else {
                        Err(ParquetReaderError::FilterError(format!(
                            "Column {col_name} is not an integer array"
                        )))
                    }
                }
                _ => Err(ParquetReaderError::FilterError(format!(
                    "Unsupported literal type for equality comparison: {literal_value:?}"
                ))),
            }
        }

        Expr::Gt(col_name, literal_value) => {
            // Get the column
            let col_idx = batch.schema().index_of(col_name).map_err(|_| {
                ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
            })?;
            let column = batch.column(col_idx);

            // Create a boolean mask based on greater than comparison
            match literal_value {
                LiteralValue::Int(n) => {
                    if let Some(int_array) =
                        column.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        let mut values = Vec::with_capacity(int_array.len());
                        for i in 0..int_array.len() {
                            if int_array.is_null(i) {
                                values.push(false);
                            } else {
                                values.push(int_array.value(i) as i64 > *n);
                            }
                        }
                        Ok(BooleanArray::from(values))
                    } else if let Some(int_array) =
                        column.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        let mut values = Vec::with_capacity(int_array.len());
                        for i in 0..int_array.len() {
                            if int_array.is_null(i) {
                                values.push(false);
                            } else {
                                values.push(int_array.value(i) > *n);
                            }
                        }
                        Ok(BooleanArray::from(values))
                    } else {
                        Err(ParquetReaderError::FilterError(format!(
                            "Column {col_name} is not an integer array"
                        )))
                    }
                }
                _ => Err(ParquetReaderError::FilterError(format!(
                    "Unsupported literal type for greater than comparison: {literal_value:?}"
                ))),
            }
        }

        // Other expression types would be implemented similarly
        _ => Err(ParquetReaderError::FilterError(format!(
            "Unsupported filter expression: {expr:?}"
        ))),
    }
}

/// Applies logical AND between two boolean arrays
fn apply_logical_and(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParquetReaderError::FilterError(
            "Boolean arrays have different lengths".to_string(),
        ));
    }

    let mut values = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        let left_val = if left.is_null(i) {
            false
        } else {
            left.value(i)
        };
        let right_val = if right.is_null(i) {
            false
        } else {
            right.value(i)
        };
        values.push(left_val && right_val);
    }

    Ok(BooleanArray::from(values))
}

/// Applies logical OR between two boolean arrays
fn apply_logical_or(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParquetReaderError::FilterError(
            "Boolean arrays have different lengths".to_string(),
        ));
    }

    let mut values = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        let left_val = if left.is_null(i) {
            false
        } else {
            left.value(i)
        };
        let right_val = if right.is_null(i) {
            false
        } else {
            right.value(i)
        };
        values.push(left_val || right_val);
    }

    Ok(BooleanArray::from(values))
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
    if batch.num_rows() != mask.len() {
        return Err(ParquetReaderError::FilterError(
            "Mask length doesn't match batch row count".to_string(),
        ));
    }

    // Apply the filter to all columns
    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow_filter(col, mask))
        .collect::<arrow::error::Result<_>>()
        .map_err(ParquetReaderError::ArrowError)?;

    // Create a new record batch with filtered data
    RecordBatch::try_new(batch.schema(), filtered_columns).map_err(ParquetReaderError::ArrowError)
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
    // Get all columns required by the filter expression
    let mut all_required_columns = expr.required_columns();

    // Add any additional columns requested for projection
    if let Some(cols) = columns {
        for col in cols {
            all_required_columns.insert(col.clone());
        }
    }

    // Read the file with schema projection containing required columns
    let batches = utils::read_parquet(path, None, None)?;

    // Filter each batch
    let mut filtered_batches = Vec::new();
    for batch in batches {
        // Evaluate the filter expression
        let mask = evaluate_expr(&batch, expr)?;

        // Apply the filter to the batch
        let filtered_batch = filter_record_batch(&batch, &mask)?;

        // Only add non-empty batches
        if filtered_batch.num_rows() > 0 {
            filtered_batches.push(filtered_batch);
        }
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
pub fn create_pnr_filter(pnrs: &HashSet<String>) -> Expr {
    let values = pnrs
        .iter()
        .map(|s| LiteralValue::String(s.clone()))
        .collect();

    Expr::In("PNR".to_string(), values)
}
