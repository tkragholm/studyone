//! Schema for the Danish Death Register (DOD)
//!
//! This module defines the schema for the Danish Death Register,
//! which contains information about deaths in Denmark.

use arrow::datatypes::{DataType, Field, Schema};

/// Create the schema for the Danish Death Register (DOD)
#[must_use] pub fn dod_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DODDATO", DataType::Utf8, true),
    ])
}

/// Create schema for standardized version of DOD register data
#[must_use] pub fn dod_standardized_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DEATH_DATE", DataType::Date32, true),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dod_schema() {
        let schema = dod_schema();
        
        // Verify PNR field
        let pnr_field = schema.field_with_name("PNR").unwrap();
        assert_eq!(pnr_field.data_type(), &DataType::Utf8);
        assert!(!pnr_field.is_nullable());
        
        // Verify DODDATO field
        let date_field = schema.field_with_name("DODDATO").unwrap();
        assert_eq!(date_field.data_type(), &DataType::Utf8);
        assert!(date_field.is_nullable());
    }

    #[test]
    fn test_dod_standardized_schema() {
        let schema = dod_standardized_schema();
        
        // Verify PNR field
        let pnr_field = schema.field_with_name("PNR").unwrap();
        assert_eq!(pnr_field.data_type(), &DataType::Utf8);
        assert!(!pnr_field.is_nullable());
        
        // Verify DEATH_DATE field
        let date_field = schema.field_with_name("DEATH_DATE").unwrap();
        assert_eq!(date_field.data_type(), &DataType::Date32);
        assert!(date_field.is_nullable());
    }
}