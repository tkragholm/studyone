//! UDDF schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for UDDF data
///
/// The UDDF (Uddannelse) registry contains educational information.
pub fn uddf_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("HFAUDD", DataType::Utf8, true),    // Highest completed education
        Field::new("HFAUDD_NIVEAU", DataType::Utf8, true), // Education level
        Field::new("HFAUDD_DATO", DataType::Date32, true), // Completion date
        Field::new("IGANGV_UDD", DataType::Utf8, true), // Ongoing education
        Field::new("IGANGV_NIVEAU", DataType::Utf8, true), // Ongoing education level
        Field::new("IGANGV_START", DataType::Date32, true), // Start date
        Field::new("YEAR", DataType::Int16, true),
    ]))
}