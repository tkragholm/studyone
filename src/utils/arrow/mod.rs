//! Arrow data handling utilities
//!
//! This module contains utilities for working with Arrow arrays, data types,
//! and record batches. It provides helpers for type conversion, data extraction,
//! and array manipulations.

pub mod array_utils;
pub mod conversion;
pub mod extractors;

// Re-export commonly used functions for convenience
pub use array_utils::get_column;
pub use conversion::{
    arrow_array_to_bool, arrow_array_to_date, arrow_array_to_f64, arrow_array_to_i32,
    arrow_array_to_i64, arrow_array_to_string, arrow_date_to_naive_date,
};
pub use extractors::{
    extract_boolean, extract_date32, extract_date_from_string, extract_float64,
    extract_int32, extract_int8_as_padded_string, extract_string,
};