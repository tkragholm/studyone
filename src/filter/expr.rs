//! Expression-based filtering for Parquet data
//!
//! This module provides an expression-based filtering system that
//! allows filtering Arrow record batches based on column values.

use std::collections::HashSet;

use anyhow::Context;
use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};

use arrow::compute::{and, not, or};
use arrow::record_batch::RecordBatch;

use crate::error::{ParquetReaderError, Result};
use crate::filter::core::{BatchFilter, filter_record_batch};

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

/// A filter that evaluates an expression against a record batch
#[derive(Debug, Clone)]
pub struct ExpressionFilter {
    /// The expression to evaluate
    expr: Expr,
}

impl ExpressionFilter {
    /// Create a new expression filter
    #[must_use]
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }

    /// Evaluate an expression against a record batch
    ///
    /// # Arguments
    /// * `batch` - The record batch to evaluate against
    /// * `expr` - The expression to evaluate
    ///
    /// # Returns
    /// A boolean array indicating which rows match the expression
    ///
    /// # Errors
    /// Returns an error if expression evaluation fails
    fn evaluate_expr(&self, batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
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

            Expr::And(exprs) => self.evaluate_and_expression(batch, exprs),

            Expr::Or(exprs) => self.evaluate_or_expression(batch, exprs),

            Expr::Not(expr) => self.evaluate_not_expression(batch, expr),

            Expr::Eq(col_name, literal_value) => {
                self.evaluate_eq_expression(batch, col_name, literal_value)
            }

            Expr::NotEq(col_name, literal_value) => {
                // NOT(column = value)
                let eq_result = self.evaluate_eq_expression(batch, col_name, literal_value)?;

                let result =
                    not(&eq_result).map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
                    })?
                    .clone())
            }

            Expr::Gt(col_name, literal_value) => {
                self.evaluate_gt_expression(batch, col_name, literal_value)
            }

            Expr::GtEq(col_name, literal_value) => {
                // a >= b is equivalent to NOT(a < b)
                let lt_result = self.evaluate_lt_expression(batch, col_name, literal_value)?;

                let result =
                    not(&lt_result).map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
                    })?
                    .clone())
            }

            Expr::Lt(col_name, literal_value) => {
                self.evaluate_lt_expression(batch, col_name, literal_value)
            }

            Expr::LtEq(col_name, literal_value) => {
                // a <= b is equivalent to NOT(a > b)
                let gt_result = self.evaluate_gt_expression(batch, col_name, literal_value)?;

                let result =
                    not(&gt_result).map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
                    })?
                    .clone())
            }

            Expr::IsNull(col_name) => self.evaluate_is_null_expression(batch, col_name),

            Expr::IsNotNull(col_name) => {
                // NOT(IsNull)
                let is_null_result = self.evaluate_is_null_expression(batch, col_name)?;

                let result = not(&is_null_result)
                    .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
                    })?
                    .clone())
            }

            Expr::In(col_name, values) => self.evaluate_in_expression(batch, col_name, values),

            Expr::NotIn(col_name, values) => {
                // NOT(In)
                let in_result = self.evaluate_in_expression(batch, col_name, values)?;

                let result =
                    not(&in_result).map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
                    })?
                    .clone())
            }

            Expr::Contains(col_name, substring) => {
                self.evaluate_contains_expression(batch, col_name, substring)
            }

            Expr::StartsWith(col_name, prefix) => {
                self.evaluate_starts_with_expression(batch, col_name, prefix)
            }

            Expr::EndsWith(col_name, suffix) => {
                self.evaluate_ends_with_expression(batch, col_name, suffix)
            }
        }
    }

    /// Evaluates a logical AND expression
    fn evaluate_and_expression(&self, batch: &RecordBatch, exprs: &[Expr]) -> Result<BooleanArray> {
        if exprs.is_empty() {
            return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
        }

        // Evaluate the first expression
        let mut result = self.evaluate_expr(batch, &exprs[0])?;

        // Apply AND with each subsequent expression using vectorized operations
        for expr in &exprs[1..] {
            let mask = self.evaluate_expr(batch, expr)?;

            // Use Arrow's vectorized 'and' function instead of row-by-row
            let result_ref =
                and(&result, &mask).context("Failed to apply AND operation to filter arrays")?;

            result = result_ref
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast boolean array"))?
                .clone();
        }

        Ok(result)
    }

    /// Evaluates a logical OR expression
    fn evaluate_or_expression(&self, batch: &RecordBatch, exprs: &[Expr]) -> Result<BooleanArray> {
        if exprs.is_empty() {
            return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
        }

        // Evaluate the first expression
        let mut result = self.evaluate_expr(batch, &exprs[0])?;

        // Apply OR with each subsequent expression using vectorized operations
        for expr in &exprs[1..] {
            let mask = self.evaluate_expr(batch, expr)?;

            // Use Arrow's vectorized 'or' function instead of row-by-row
            let result_ref =
                or(&result, &mask).map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

            result = result_ref
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast boolean array"))?
                .clone();
        }

        Ok(result)
    }

    /// Evaluates a logical NOT expression
    fn evaluate_not_expression(&self, batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
        let mask = self.evaluate_expr(batch, expr)?;

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
    fn evaluate_eq_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        literal_value: &LiteralValue,
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = match batch.schema().index_of(col_name) {
            Ok(idx) => idx,
            Err(_) => return crate::filter::error::column_not_found(col_name),
        };
        let column = batch.column(col_idx);

        // Create a boolean mask based on equality comparison using vectorized operations
        match literal_value {
            LiteralValue::String(s) => self.evaluate_string_eq(column, col_name, s),
            LiteralValue::Int(n) => self.evaluate_int_eq(column, col_name, *n),
            _ => crate::filter::error::filter_err(format!(
                "Unsupported literal type for equality comparison: {:?}",
                literal_value
            )),
        }
    }

    /// Evaluates equality comparison for string types
    fn evaluate_string_eq(
        &self,
        column: &arrow::array::ArrayRef,
        col_name: &str,
        s: &str,
    ) -> Result<BooleanArray> {
        use crate::filter::error::{FilterResultExt, column_type_error};
        use arrow::compute::kernels::cmp::eq;

        if let Some(str_array) = column.as_any().downcast_ref::<arrow::array::StringArray>() {
            // Create a constant array of comparison values
            let literal_array = arrow::array::StringArray::from(vec![s; str_array.len()]);

            // Use Arrow's vectorized eq function
            let result = eq(str_array, &literal_array)
                .with_filter_context("Failed to apply equality comparison")?;

            // Manual unwrapping to avoid nested Results
            match result.as_any().downcast_ref::<BooleanArray>() {
                Some(array) => Ok(array.clone()),
                None => crate::filter::error::filter_err("Failed to downcast boolean array"),
            }
        } else {
            column_type_error(col_name, "string")
        }
    }

    /// Evaluates equality comparison for integer types
    fn evaluate_int_eq(
        &self,
        column: &arrow::array::ArrayRef,
        col_name: &str,
        n: i64,
    ) -> Result<BooleanArray> {
        use arrow::compute::kernels::cmp::eq;

        if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
            // Convert i64 to i32 safely
            if let Ok(n_i32) = i32::try_from(n) {
                // Create a constant array of comparison values
                let literal_array = Int32Array::from(vec![n_i32; int_array.len()]);

                // Use Arrow's vectorized eq function
                let result = eq(int_array, &literal_array)
                    .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
                    })?
                    .clone())
            } else {
                // If the i64 value doesn't fit in i32, the equality will always be false
                Ok(arrow::array::BooleanArray::from(vec![
                    false;
                    int_array.len()
                ]))
            }
        } else if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            // Create a constant array of comparison values
            let literal_array = Int64Array::from(vec![n; int_array.len()]);

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
    fn evaluate_gt_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        literal_value: &LiteralValue,
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
        })?;
        let column = batch.column(col_idx);

        // Create a boolean mask based on greater than comparison using vectorized operations
        match literal_value {
            LiteralValue::Int(n) => self.evaluate_int_gt(column, col_name, *n),
            _ => Err(anyhow::anyhow!(
                "Unsupported literal type for greater than comparison: {literal_value:?}"
            )),
        }
    }

    /// Evaluates less than comparison expression
    fn evaluate_lt_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        literal_value: &LiteralValue,
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
        })?;
        let column = batch.column(col_idx);

        // Create a boolean mask based on less than comparison using vectorized operations
        match literal_value {
            LiteralValue::Int(n) => self.evaluate_int_lt(column, col_name, *n),
            _ => Err(anyhow::anyhow!(
                "Unsupported literal type for less than comparison: {literal_value:?}"
            )),
        }
    }

    /// Evaluates greater than comparison for integer types
    fn evaluate_int_gt(
        &self,
        column: &arrow::array::ArrayRef,
        col_name: &str,
        n: i64,
    ) -> Result<BooleanArray> {
        use arrow::compute::kernels::cmp::gt;

        if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
            // Convert i64 to i32 safely
            if let Ok(n_i32) = i32::try_from(n) {
                // Create a constant array of comparison values
                let literal_array = Int32Array::from(vec![n_i32; int_array.len()]);

                // Use Arrow's vectorized gt function
                let result = gt(int_array, &literal_array)
                    .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
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
        } else if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            // Create a constant array of comparison values
            let literal_array = Int64Array::from(vec![n; int_array.len()]);

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

    /// Evaluates less than comparison for integer types
    fn evaluate_int_lt(
        &self,
        column: &arrow::array::ArrayRef,
        col_name: &str,
        n: i64,
    ) -> Result<BooleanArray> {
        use arrow::compute::kernels::cmp::lt;

        if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
            // Convert i64 to i32 safely
            if let Ok(n_i32) = i32::try_from(n) {
                // Create a constant array of comparison values
                let literal_array = Int32Array::from(vec![n_i32; int_array.len()]);

                // Use Arrow's vectorized lt function
                let result = lt(int_array, &literal_array)
                    .map_err(|e| ParquetReaderError::FilterError(e.to_string()))?;

                Ok(result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ParquetReaderError::FilterError(
                            "Failed to downcast boolean array".to_string(),
                        )
                    })?
                    .clone())
            } else {
                // If i64 value is too large for i32, for "a < b" the result depends on the sign
                // If n is positive, all values are less, so all true
                // If n is negative, no values are less, so all false
                let result = if n > 0 {
                    vec![true; int_array.len()]
                } else {
                    vec![false; int_array.len()]
                };
                Ok(arrow::array::BooleanArray::from(result))
            }
        } else if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            // Create a constant array of comparison values
            let literal_array = Int64Array::from(vec![n; int_array.len()]);

            // Use Arrow's vectorized lt function
            let result = lt(int_array, &literal_array)
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

    /// Evaluates IS NULL expression
    fn evaluate_is_null_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
        })?;
        let column = batch.column(col_idx);

        // Create a boolean array where true means the value is null
        let mut is_null = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            is_null.push(column.is_null(i));
        }

        Ok(BooleanArray::from(is_null))
    }

    /// Evaluates IN expression
    fn evaluate_in_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        values: &[LiteralValue],
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
        })?;
        let column = batch.column(col_idx);

        // Convert values to a set for efficient lookup
        let str_values: HashSet<String> = values
            .iter()
            .filter_map(|v| match v {
                LiteralValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect();

        let int_values: HashSet<i64> = values
            .iter()
            .filter_map(|v| match v {
                LiteralValue::Int(n) => Some(*n),
                _ => None,
            })
            .collect();

        // Check if we have string values to match
        if !str_values.is_empty() {
            if let Some(str_array) = column.as_any().downcast_ref::<StringArray>() {
                let mut in_set = Vec::with_capacity(str_array.len());
                for i in 0..str_array.len() {
                    if str_array.is_null(i) {
                        in_set.push(false);
                    } else {
                        in_set.push(str_values.contains(str_array.value(i)));
                    }
                }
                return Ok(BooleanArray::from(in_set));
            }
        }

        // Check if we have integer values to match
        if !int_values.is_empty() {
            if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
                let mut in_set = Vec::with_capacity(int_array.len());
                for i in 0..int_array.len() {
                    if int_array.is_null(i) {
                        in_set.push(false);
                    } else {
                        in_set.push(int_values.contains(&(int_array.value(i) as i64)));
                    }
                }
                return Ok(BooleanArray::from(in_set));
            } else if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
                let mut in_set = Vec::with_capacity(int_array.len());
                for i in 0..int_array.len() {
                    if int_array.is_null(i) {
                        in_set.push(false);
                    } else {
                        in_set.push(int_values.contains(&int_array.value(i)));
                    }
                }
                return Ok(BooleanArray::from(in_set));
            }
        }

        // If we reach here, either the column type doesn't match any of our value types
        // or we have an empty set of values, in which case nothing matches
        Err(anyhow::anyhow!(
            "Unsupported column or literal types for IN expression: column: {col_name}, values: {values:?}"
        ))
    }

    /// Evaluates CONTAINS expression
    fn evaluate_contains_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        substring: &str,
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
        })?;
        let column = batch.column(col_idx);

        if let Some(str_array) = column.as_any().downcast_ref::<StringArray>() {
            let mut contains = Vec::with_capacity(str_array.len());
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    contains.push(false);
                } else {
                    contains.push(str_array.value(i).contains(substring));
                }
            }
            return Ok(BooleanArray::from(contains));
        }

        Err(anyhow::anyhow!("Column {col_name} is not a string array"))
    }

    /// Evaluates STARTS_WITH expression
    fn evaluate_starts_with_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        prefix: &str,
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
        })?;
        let column = batch.column(col_idx);

        if let Some(str_array) = column.as_any().downcast_ref::<StringArray>() {
            let mut starts_with = Vec::with_capacity(str_array.len());
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    starts_with.push(false);
                } else {
                    starts_with.push(str_array.value(i).starts_with(prefix));
                }
            }
            return Ok(BooleanArray::from(starts_with));
        }

        Err(anyhow::anyhow!("Column {col_name} is not a string array"))
    }

    /// Evaluates ENDS_WITH expression
    fn evaluate_ends_with_expression(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        suffix: &str,
    ) -> Result<BooleanArray> {
        // Get the column
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            ParquetReaderError::FilterError(format!("Column {col_name} not found in batch"))
        })?;
        let column = batch.column(col_idx);

        if let Some(str_array) = column.as_any().downcast_ref::<StringArray>() {
            let mut ends_with = Vec::with_capacity(str_array.len());
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    ends_with.push(false);
                } else {
                    ends_with.push(str_array.value(i).ends_with(suffix));
                }
            }
            return Ok(BooleanArray::from(ends_with));
        }

        Err(anyhow::anyhow!("Column {col_name} is not a string array"))
    }
}

impl BatchFilter for ExpressionFilter {
    fn filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Evaluate the expression
        let mask = self.evaluate_expr(batch, &self.expr)?;

        // Apply the filter
        filter_record_batch(batch, &mask)
    }

    fn required_columns(&self) -> HashSet<String> {
        self.expr.required_columns()
    }
}

/// Helper function to create equality filter for a column
///
/// # Arguments
/// * `column` - The column name
/// * `value` - The value to match
///
/// # Returns
/// An expression that matches records where column equals value
#[must_use]
pub fn eq_filter(column: &str, value: LiteralValue) -> Expr {
    Expr::Eq(column.to_string(), value)
}

/// Helper function to create IN filter for a column
///
/// # Arguments
/// * `column` - The column name
/// * `values` - The values to match
///
/// # Returns
/// An expression that matches records where column is in values
#[must_use]
pub fn in_filter(column: &str, values: Vec<LiteralValue>) -> Expr {
    Expr::In(column.to_string(), values)
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
