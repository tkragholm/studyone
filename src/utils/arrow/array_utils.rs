//! Utilities for working with Arrow arrays.
//!
//! This module provides utility functions for safely extracting and converting
//! data from Arrow arrays, handling type conversion and error handling.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use log::{info, warn};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::schema::adapt::{DateFormatConfig, compatibility, conversions};

/// Get a column from a record batch with automatic type adaptation
///
/// This function provides a convenient way to extract a column from a record batch,
/// while ensuring it has the expected data type. If the column has a different type,
/// it will attempt to convert it to the expected type using the schema adaptation system.
///
/// # Arguments
///
/// * `batch` - The record batch containing the column
/// * `column_name` - The name of the column to extract
/// * `expected_type` - The expected data type for the column
/// * `required` - Whether the column is required (error if missing) or optional (None if missing)
///
/// # Returns
///
/// * `Ok(Some(ArrayRef))` - The column array (converted if necessary) if found
/// * `Ok(None)` - If the column is not found and `required` is false
/// * `Err(Error)` - If the column is not found and `required` is true, or if type conversion fails
pub fn get_column(
    batch: &RecordBatch,
    column_name: &str,
    expected_type: &DataType,
    required: bool,
) -> Result<Option<ArrayRef>> {
    // Try to find the column
    let idx = if let Ok(idx) = batch.schema().index_of(column_name) {
        idx
    } else {
        if required {
            return Err(Error::ColumnNotFound {
                column: column_name.to_string(),
            }
            .into());
        }
        warn!("Column '{column_name}' not found in record batch");
        return Ok(None);
    };

    // Get the column
    let column = batch.column(idx);
    let actual_type = column.data_type();

    // If types already match, return the column as is
    if actual_type == expected_type {
        return Ok(Some(column.clone()));
    }

    // Types don't match, try to convert
    info!("Converting column '{column_name}' from {actual_type:?} to {expected_type:?}");

    // Use appropriate conversion based on the target type
    let date_config = DateFormatConfig::default();
    let converted = match expected_type {
        DataType::Date32 | DataType::Date64 => {
            // Date conversions need special handling
            match conversions::convert_array(column, expected_type, &date_config) {
                Ok(converted) => converted,
                Err(err) => {
                    warn!("Failed to convert column '{column_name}' to {expected_type:?}: {err}");
                    // Create a null array as fallback
                    match conversions::create_null_array(expected_type, batch.num_rows()) {
                        Ok(null_array) => null_array,
                        Err(_) => Arc::new(arrow::array::NullArray::new(batch.num_rows())),
                    }
                }
            }
        }
        _ if compatibility::is_numeric(expected_type) && compatibility::is_numeric(actual_type) => {
            // Numeric conversions should work with arrow's cast
            match arrow::compute::kernels::cast::cast(column, expected_type) {
                Ok(converted) => converted,
                Err(err) => {
                    warn!(
                        "Failed to convert column '{column_name}' from {actual_type:?} to {expected_type:?}: {err}"
                    );
                    // Create a null array as fallback
                    match conversions::create_null_array(expected_type, batch.num_rows()) {
                        Ok(null_array) => null_array,
                        Err(_) => Arc::new(arrow::array::NullArray::new(batch.num_rows())),
                    }
                }
            }
        }
        _ => {
            // Try generic conversion with our adapter system
            match conversions::convert_array(column, expected_type, &date_config) {
                Ok(converted) => converted,
                Err(err) => {
                    warn!("Failed to convert column '{column_name}' to {expected_type:?}: {err}");
                    // Create a null array as fallback
                    match conversions::create_null_array(expected_type, batch.num_rows()) {
                        Ok(null_array) => null_array,
                        Err(_) => Arc::new(arrow::array::NullArray::new(batch.num_rows())),
                    }
                }
            }
        }
    };

    Ok(Some(converted))
}

/// Downcast a column to a specific array type with clear error messages
///
/// # Type Parameters
///
/// * `A` - The target array type to downcast to
///
/// # Arguments
///
/// * `array` - The array reference to downcast
/// * `column_name` - The name of the column (for error messages)
/// * `expected_type_name` - A human-readable name of the expected type (for error messages)
///
/// # Returns
///
/// * `Ok(&A)` - The downcasted array reference
/// * `Err(Error)` - If the downcast fails
pub fn downcast_array<'a, A: Array + 'static>(
    array: &'a ArrayRef,
    column_name: &str,
    expected_type_name: &str,
) -> Result<&'a A> {
    // First get the reference
    let result = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::InvalidDataType {
            column: column_name.to_string(),
            expected: expected_type_name.to_string(),
        });

    // Then convert from our custom error to anyhow::Error
    result.map_err(std::convert::Into::into)
}

/// Get the column index by name from a record batch
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to find
///
/// # Returns
/// The index of the column
///
/// # Errors
/// Returns an error if the column does not exist
pub fn get_column_index(batch: &RecordBatch, column_name: &str) -> Result<usize> {
    batch.schema().index_of(column_name).map_err(|_| {
        Error::ColumnNotFound {
            column: column_name.to_string(),
        }
        .into()
    })
}

/// Get a column from a record batch by name
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to find
///
/// # Returns
/// The column as an `ArrayRef`
///
/// # Errors
/// Returns an error if the column does not exist
pub fn get_column_by_name(batch: &RecordBatch, column_name: &str) -> Result<ArrayRef> {
    let idx = get_column_index(batch, column_name)?;
    Ok(batch.column(idx).clone())
}
