//! Date filtering functionality for Parquet data
//!
//! This module provides specialized filtering by date ranges and other date-related operations.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Array, ArrayRef, BooleanArray, Date32Array, Int32Array};
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::boolean;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};

use crate::error::{Error, Result};
use crate::filter::core::{filter_record_batch, BatchFilter};
use crate::filter::expr::{Expr, ExpressionFilter, LiteralValue};

/// A filter that includes only rows with dates in a specified range
#[derive(Debug, Clone)]
pub struct DateRangeFilter {
    /// The name of the date column
    date_column: String,
    
    /// The start date (inclusive)
    start_date: Option<NaiveDate>,
    
    /// The end date (inclusive)
    end_date: Option<NaiveDate>,
}

impl DateRangeFilter {
    /// Create a new date range filter
    ///
    /// # Arguments
    /// * `date_column` - The name of the date column
    /// * `start_date` - Optional start date (inclusive)
    /// * `end_date` - Optional end date (inclusive)
    ///
    /// # Returns
    /// A new date range filter
    #[must_use]
    pub fn new(
        date_column: String,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Self {
        Self {
            date_column,
            start_date,
            end_date,
        }
    }
}

impl BatchFilter for DateRangeFilter {
    fn filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Find the date column
        let date_idx = batch.schema().index_of(&self.date_column).map_err(|e| {
            Error::ValidationError(format!("Date column '{}' not found: {e}", self.date_column))
        })?;

        let date_array = batch.column(date_idx);
        let date_array = date_array
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| {
                Error::ValidationError(format!("Column '{}' is not a Date32 array", self.date_column))
            })?;

        // Use Arrow's compute functions for vectorized comparison
        let mut in_range = BooleanArray::from(vec![true; batch.num_rows()]);

        // Apply start date filter if specified
        if let Some(start) = self.start_date {
            let start_days = start.num_days_from_ce() - 719_163;

            // Create array of the start date for vectorized comparison
            let start_date_array = Date32Array::from(vec![start_days; batch.num_rows()]);

            // Vectorized comparison: date >= start_date
            let ge_result =
                cmp::gt_eq(date_array, &start_date_array).with_context(|| "Failed to compare dates")?;

            // Combine with existing mask
            in_range = boolean::and(&in_range, &ge_result)
                .with_context(|| "Failed to combine date filters")?;
        }

        // Apply end date filter if specified
        if let Some(end) = self.end_date {
            let end_days = end.num_days_from_ce() - 719_163;

            // Create array of the end date for vectorized comparison
            let end_date_array = Date32Array::from(vec![end_days; batch.num_rows()]);

            // Vectorized comparison: date <= end_date
            let le_result =
                cmp::lt_eq(date_array, &end_date_array).with_context(|| "Failed to compare dates")?;

            // Combine with existing mask
            in_range = boolean::and(&in_range, &le_result)
                .with_context(|| "Failed to combine date filters")?;
        }

        // Handle nulls in the date column - exclude rows with null dates
        // Create a boolean array where true means the date is not null
        let mut not_null_values = Vec::with_capacity(date_array.len());
        for i in 0..date_array.len() {
            not_null_values.push(!date_array.is_null(i));
        }
        let null_mask = BooleanArray::from(not_null_values);

        // Combine with date range mask
        let mask = boolean::and(&in_range, &null_mask).with_context(|| "Failed to combine masks")?;

        // Apply the filter to the batch
        filter_record_batch(batch, &mask)
    }
    
    fn required_columns(&self) -> HashSet<String> {
        let mut cols = HashSet::new();
        cols.insert(self.date_column.clone());
        cols
    }
}

/// Create an expression filter for a date range
///
/// # Arguments
/// * `date_column` - The name of the date column
/// * `start_date` - Optional start date (inclusive)
/// * `end_date` - Optional end date (inclusive)
///
/// # Returns
/// An expression filter for the date range
#[must_use]
pub fn create_date_range_expression_filter(
    date_column: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> ExpressionFilter {
    let mut conditions = Vec::new();
    
    // Add start date condition if specified
    if let Some(start) = start_date {
        let start_days = start.num_days_from_ce() - 719_163;
        conditions.push(Expr::GtEq(
            date_column.to_string(),
            LiteralValue::Date(start_days),
        ));
    }
    
    // Add end date condition if specified
    if let Some(end) = end_date {
        let end_days = end.num_days_from_ce() - 719_163;
        conditions.push(Expr::LtEq(
            date_column.to_string(),
            LiteralValue::Date(end_days),
        ));
    }
    
    // Add not null condition
    conditions.push(Expr::IsNotNull(date_column.to_string()));
    
    // Combine all conditions with AND
    let expr = if conditions.is_empty() {
        Expr::AlwaysTrue
    } else if conditions.len() == 1 {
        conditions.remove(0)
    } else {
        Expr::And(conditions)
    };
    
    ExpressionFilter::new(expr)
}

/// Add a year column to a record batch based on a date column
///
/// # Arguments
/// * `batch` - The record batch to augment
/// * `date_column` - The name of the date column
///
/// # Returns
/// A new record batch with a year column added
///
/// # Errors
/// Returns an error if adding the year column fails
pub fn add_year_column(batch: &RecordBatch, date_column: &str) -> Result<RecordBatch> {
    // Find the date column
    let date_idx = batch.schema().index_of(date_column).map_err(|e| {
        Error::ValidationError(format!("Date column '{date_column}' not found: {e}"))
    })?;

    let date_array = batch.column(date_idx);
    let date_array = date_array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            Error::ValidationError(format!("Column '{date_column}' is not a Date32 array"))
        })?;

    // Create a new Int32Array with year values - use arrow computation logic
    let mut year_values = Vec::with_capacity(batch.num_rows());

    // Implement this vectorized if Arrow provides a function, otherwise do it per-element
    for i in 0..date_array.len() {
        if date_array.is_null(i) {
            year_values.push(None);
        } else {
            let days = date_array.value(i);
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719_163).unwrap_or_default();
            year_values.push(Some(date.year()));
        }
    }

    // Create the Int32Array for the years
    let year_array = Int32Array::from(year_values);

    // Create a new field for the year column
    let year_field = Field::new("year", DataType::Int32, true);

    // Create a new schema by adding the year field
    let schema = batch.schema();
    let fields = schema.fields();
    let mut field_vec = fields.to_vec();
    field_vec.push(Arc::new(year_field));
    let new_schema = Arc::new(Schema::new(field_vec));

    // Create a new record batch with all the original columns plus the year column
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(year_array));

    // Create a new record batch with the new schema and columns
    RecordBatch::try_new(new_schema, columns).map_err(|e| {
        Error::ArrowError(format!(
            "Failed to create record batch with year column: {e}"
        ))
        .into()
    })
}

/// Filter batches by year
///
/// # Arguments
/// * `batches` - The record batches to filter
/// * `date_column` - The name of the date column
/// * `year` - The year to filter by
///
/// # Returns
/// A vector of filtered record batches
///
/// # Errors
/// Returns an error if filtering fails
pub fn filter_by_year(
    batches: &[RecordBatch],
    date_column: &str,
    year: i32,
) -> Result<Vec<RecordBatch>> {
    // Create start and end dates for the year
    let start_date = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
    
    // Create a date range filter
    let date_filter = Arc::new(DateRangeFilter::new(
        date_column.to_string(),
        Some(start_date),
        Some(end_date),
    ));
    
    // Apply the filter to each batch
    let mut filtered_batches = Vec::with_capacity(batches.len());
    
    for batch in batches {
        let filtered_batch = date_filter.filter(batch)?;
        
        // Only add non-empty batches
        if filtered_batch.num_rows() > 0 {
            filtered_batches.push(filtered_batch);
        }
    }
    
    Ok(filtered_batches)
}