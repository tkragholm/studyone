//! A reusable Arrow `RecordBatch` filter engine
//! Supports typed, composable, expression-based filters with Parquet integration.

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{and, filter as filter_batch, not, or};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::fs::File;

#[derive(Debug, Clone)]
pub enum StringFilter {
    Eq(String),
    Neq(String),
    Contains(String),
    StartsWith(String),
    EndsWith(String),
}

#[derive(Debug, Clone)]
pub enum IntFilter {
    Eq(i64),
    Neq(i64),
    Gt(i64),
    Gte(i64),
    Lt(i64),
    Lte(i64),
}

#[derive(Debug, Clone)]
pub enum ColumnFilter {
    String(StringFilter),
    Int(IntFilter),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Filter {
        column: String,
        filter: ColumnFilter,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    In(String, Vec<String>), // Check if column value is in set of values
    AlwaysTrue,              // Always evaluates to true
}

impl Expr {
    #[must_use]
    pub fn and(self, rhs: Self) -> Self {
        Self::And(Box::new(self), Box::new(rhs))
    }

    #[must_use]
    pub fn or(self, rhs: Self) -> Self {
        Self::Or(Box::new(self), Box::new(rhs))
    }

    #[must_use]
    pub fn not(self) -> Self {
        Self::Not(Box::new(self))
    }

    #[must_use]
    pub const fn always_true() -> Self {
        Self::AlwaysTrue
    }

    #[must_use]
    pub fn required_columns(&self) -> HashSet<String> {
        let mut set = HashSet::new();
        self.collect_columns(&mut set);
        set
    }

    fn collect_columns(&self, set: &mut HashSet<String>) {
        match self {
            Self::Filter { column, .. } => {
                set.insert(column.clone());
            }
            Self::In(column, _) => {
                set.insert(column.clone());
            }
            Self::And(lhs, rhs) | Self::Or(lhs, rhs) => {
                lhs.collect_columns(set);
                rhs.collect_columns(set);
            }
            Self::Not(inner) => {
                inner.collect_columns(set);
            }
            Self::AlwaysTrue => {
                // No columns needed for AlwaysTrue
            }
        }
    }
}

#[must_use]
pub fn col(name: &str) -> ColumnBuilder {
    ColumnBuilder {
        name: name.to_string(),
    }
}

pub struct ColumnBuilder {
    name: String,
}

impl ColumnBuilder {
    pub fn eq(self, val: impl Into<ColumnFilter>) -> Expr {
        Expr::Filter {
            column: self.name,
            filter: val.into(),
        }
    }

    #[must_use]
    pub fn in_list(self, values: Vec<String>) -> Expr {
        Expr::In(self.name, values)
    }

    #[must_use]
    pub fn gt(self, val: i64) -> Expr {
        Expr::Filter {
            column: self.name,
            filter: ColumnFilter::Int(IntFilter::Gt(val)),
        }
    }

    #[must_use]
    pub fn lt(self, val: i64) -> Expr {
        Expr::Filter {
            column: self.name,
            filter: ColumnFilter::Int(IntFilter::Lt(val)),
        }
    }

    #[must_use]
    pub fn starts_with(self, val: &str) -> Expr {
        Expr::Filter {
            column: self.name,
            filter: ColumnFilter::String(StringFilter::StartsWith(val.into())),
        }
    }

    #[must_use]
    pub fn ends_with(self, val: &str) -> Expr {
        Expr::Filter {
            column: self.name,
            filter: ColumnFilter::String(StringFilter::EndsWith(val.into())),
        }
    }

    #[must_use]
    pub fn contains(self, val: &str) -> Expr {
        Expr::Filter {
            column: self.name,
            filter: ColumnFilter::String(StringFilter::Contains(val.into())),
        }
    }
}

impl From<StringFilter> for ColumnFilter {
    fn from(f: StringFilter) -> Self {
        Self::String(f)
    }
}

impl From<IntFilter> for ColumnFilter {
    fn from(f: IntFilter) -> Self {
        Self::Int(f)
    }
}

impl From<&str> for StringFilter {
    fn from(s: &str) -> Self {
        Self::Eq(s.to_string())
    }
}

impl From<String> for StringFilter {
    fn from(s: String) -> Self {
        Self::Eq(s)
    }
}

// Add a direct implementation for string to ColumnFilter
impl From<&str> for ColumnFilter {
    fn from(s: &str) -> Self {
        Self::String(StringFilter::Eq(s.to_string()))
    }
}

impl From<String> for ColumnFilter {
    fn from(s: String) -> Self {
        Self::String(StringFilter::Eq(s))
    }
}

pub fn evaluate_expr(batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
    match expr {
        Expr::Filter { column, filter } => filter_mask_from_column_filter(batch, column, filter),
        Expr::And(lhs, rhs) => Ok(and(
            &evaluate_expr(batch, lhs)?,
            &evaluate_expr(batch, rhs)?,
        )?),
        Expr::Or(lhs, rhs) => Ok(or(
            &evaluate_expr(batch, lhs)?,
            &evaluate_expr(batch, rhs)?,
        )?),
        Expr::Not(inner) => Ok(not(&evaluate_expr(batch, inner)?)?),
        Expr::In(column, values) => evaluate_in_expr(batch, column, values),
        Expr::AlwaysTrue => {
            // Create a boolean array with all true values
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        }
    }
}

pub fn evaluate_in_expr(
    batch: &RecordBatch,
    column: &str,
    values: &[String],
) -> Result<BooleanArray> {
    let index = batch.schema().index_of(column)?;
    let array = batch.column(index);

    // Handle string arrays
    if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
        // Create a HashSet from values for O(1) lookups
        let value_set: HashSet<&str> = values.iter().map(std::string::String::as_str).collect();

        // Map each value in the column to true if it's in value_set, false otherwise
        let result_iter = str_array
            .iter()
            .map(|opt_str| opt_str.map(|s| value_set.contains(s)));

        // Collect into a BooleanArray
        Ok(result_iter.collect())
    } else {
        Err(ArrowError::ComputeError(format!(
            "Column '{column}' is not a StringArray, cannot perform IN operation with string values"
        )))
    }
}

pub fn filter_mask_from_column_filter(
    batch: &RecordBatch,
    column: &str,
    filter: &ColumnFilter,
) -> Result<BooleanArray> {
    match filter {
        ColumnFilter::String(f) => string_mask_from_filter(batch, column, f),
        ColumnFilter::Int(f) => int_mask_from_filter(batch, column, f),
    }
}

pub fn string_mask_from_filter(
    batch: &RecordBatch,
    column: &str,
    filter: &StringFilter,
) -> Result<BooleanArray> {
    let index = batch.schema().index_of(column)?;
    let array = batch.column(index);
    let str_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::ComputeError("Expected StringArray".into()))?;

    // Convert the filter value to a StringArray with a single element
    // This array will be broadcast to match the length of the str_array
    match filter {
        StringFilter::Eq(val) => {
            // Create a scalar value from the filter value
            let scalar = StringArray::new_scalar(val.clone());
            eq(str_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("String equality operation failed: {e}"))
            })
        }
        StringFilter::Neq(val) => {
            // Create a scalar value from the filter value
            let scalar = StringArray::new_scalar(val.clone());
            neq(str_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("String inequality operation failed: {e}"))
            })
        }
        StringFilter::Contains(substr) => {
            // Create a boolean array from direct comparison using the iterator-based construction
            let result_iter = str_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.contains(substr)));

            // Collect into a BooleanArray
            Ok(result_iter.collect())
        }
        StringFilter::StartsWith(prefix) => {
            // Create a boolean array from direct comparison using the iterator-based construction
            let result_iter = str_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.starts_with(prefix)));

            // Collect into a BooleanArray
            Ok(result_iter.collect())
        }
        StringFilter::EndsWith(suffix) => {
            // Create a boolean array from direct comparison using the iterator-based construction
            let result_iter = str_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.ends_with(suffix)));

            // Collect into a BooleanArray
            Ok(result_iter.collect())
        }
    }
}

pub fn int_mask_from_filter(
    batch: &RecordBatch,
    column: &str,
    filter: &IntFilter,
) -> Result<BooleanArray> {
    let index = batch.schema().index_of(column)?;
    let array = batch.column(index);

    // Special handling for date columns - check if this column is a DATE type
    let binding = batch.schema();
    let field = binding.field(index);
    let is_date = matches!(
        field.data_type(),
        arrow::datatypes::DataType::Date32 | arrow::datatypes::DataType::Date64
    );

    // For date columns, we'll directly use the Int32Array since in Arrow,
    // dates are internally stored as days since epoch as i32 values
    if is_date {
        // Treat any date column as an Int32Array
        let int_array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
            ArrowError::ComputeError("Failed to interpret DATE column as Int32Array".into())
        })?;

        // Convert i64 filter value to i32 for comparison with dates
        let val_i32 = match filter {
            IntFilter::Eq(v) => *v as i32,
            IntFilter::Neq(v) => *v as i32,
            IntFilter::Gt(v) => *v as i32,
            IntFilter::Gte(v) => *v as i32,
            IntFilter::Lt(v) => *v as i32,
            IntFilter::Lte(v) => *v as i32,
        };

        // Create a scalar value from the filter value
        let scalar = Int32Array::new_scalar(val_i32);

        match filter {
            IntFilter::Eq(_) => eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Date equality operation failed: {e}"))
            }),
            IntFilter::Neq(_) => neq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Date inequality operation failed: {e}"))
            }),
            IntFilter::Gt(_) => gt(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Date greater than operation failed: {e}"))
            }),
            IntFilter::Gte(_) => gt_eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Date greater than or equal operation failed: {e}"
                ))
            }),
            IntFilter::Lt(_) => lt(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Date less than operation failed: {e}"))
            }),
            IntFilter::Lte(_) => lt_eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Date less than or equal operation failed: {e}"))
            }),
        }
    }
    // Standard numeric column handling
    else if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
        // Convert i64 filter value to i32 for comparison
        let val_i32 = match filter {
            IntFilter::Eq(v) => *v as i32,
            IntFilter::Neq(v) => *v as i32,
            IntFilter::Gt(v) => *v as i32,
            IntFilter::Gte(v) => *v as i32,
            IntFilter::Lt(v) => *v as i32,
            IntFilter::Lte(v) => *v as i32,
        };

        // Create a scalar value from the filter value
        let scalar = Int32Array::new_scalar(val_i32);

        match filter {
            IntFilter::Eq(_) => eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int32 equality operation failed: {e}"))
            }),
            IntFilter::Neq(_) => neq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int32 inequality operation failed: {e}"))
            }),
            IntFilter::Gt(_) => gt(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int32 greater than operation failed: {e}"))
            }),
            IntFilter::Gte(_) => gt_eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Int32 greater than or equal operation failed: {e}"
                ))
            }),
            IntFilter::Lt(_) => lt(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int32 less than operation failed: {e}"))
            }),
            IntFilter::Lte(_) => lt_eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int32 less than or equal operation failed: {e}"))
            }),
        }
    } else if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
        // Create a scalar value from the filter value
        let val_i64 = match filter {
            IntFilter::Eq(v) => *v,
            IntFilter::Neq(v) => *v,
            IntFilter::Gt(v) => *v,
            IntFilter::Gte(v) => *v,
            IntFilter::Lt(v) => *v,
            IntFilter::Lte(v) => *v,
        };

        let scalar = Int64Array::new_scalar(val_i64);

        match filter {
            IntFilter::Eq(_) => eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int64 equality operation failed: {e}"))
            }),
            IntFilter::Neq(_) => neq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int64 inequality operation failed: {e}"))
            }),
            IntFilter::Gt(_) => gt(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int64 greater than operation failed: {e}"))
            }),
            IntFilter::Gte(_) => gt_eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Int64 greater than or equal operation failed: {e}"
                ))
            }),
            IntFilter::Lt(_) => lt(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int64 less than operation failed: {e}"))
            }),
            IntFilter::Lte(_) => lt_eq(int_array, &scalar).map_err(|e| {
                ArrowError::ComputeError(format!("Int64 less than or equal operation failed: {e}"))
            }),
        }
    } else {
        Err(ArrowError::ComputeError(format!(
            "Expected Int32Array or Int64Array for column '{}', but found {:?}",
            column,
            field.data_type()
        )))
    }
}

pub fn filter_record_batch(batch: &RecordBatch, mask: &BooleanArray) -> Result<RecordBatch> {
    let filtered_columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|col| filter_batch(col.as_ref(), mask))
        .collect();
    RecordBatch::try_new(batch.schema(), filtered_columns?)
}

/// Reads a parquet file with projection based on the filter expression and additional requested columns,
/// then applies row filtering
pub fn read_and_filter_parquet(
    path: &str,
    expr: &Expr,
    additional_columns: &[String],
) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = reader_builder.schema();

    // Get all columns required by the filter expression
    let mut all_required_columns = expr.required_columns();

    // Add any additional columns requested for projection
    for col in additional_columns {
        all_required_columns.insert(col.clone());
    }

    // If no additional columns were specified and the required columns are empty,
    // we'll use all columns (no projection)
    let final_columns = if additional_columns.is_empty() && all_required_columns.is_empty() {
        vec![] // Empty vec will result in no projection
    } else {
        // If additional columns were specified but no filter columns are needed,
        // use only the additional columns
        if additional_columns.is_empty() {
            all_required_columns.into_iter().collect()
        } else {
            // If both filter and additional columns are specified, use both
            all_required_columns.into_iter().collect()
        }
    };

    // Convert column names to indices for the projection
    let projection: Vec<usize> = if final_columns.is_empty() {
        vec![] // Empty vec will result in no projection
    } else {
        final_columns
            .iter()
            .map(|name| schema.index_of(name))
            .collect::<Result<_>>()?
    };

    // Build the reader with selected columns
    let reader = if projection.is_empty() {
        reader_builder.build()?
    } else {
        let projection_mask =
            parquet::arrow::ProjectionMask::leaves(reader_builder.parquet_schema(), projection);
        reader_builder.with_projection(projection_mask).build()?
    };

    let mut results = Vec::new();

    // Process each batch
    for batch_result in reader {
        let batch = batch_result?;
        let mask = evaluate_expr(&batch, expr)?;
        let filtered = filter_record_batch(&batch, &mask)?;

        // Only add non-empty batches to results
        if filtered.num_rows() > 0 {
            results.push(filtered);
        }
    }

    Ok(results)
}
