//! Schema for the Danish Death Cause Register (DODSAARSAG)
//!
//! This module defines the schema for the Danish Death Cause Register,
//! which contains information about causes of death in Denmark.

use arrow::datatypes::{DataType, Field, Schema};

/// Create the schema for the Danish Death Cause Register (DODSAARSAG)
#[must_use] pub fn dodsaarsag_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("C_AARSAG", DataType::Utf8, true),  // Cause of death code (ICD-10)
        Field::new("C_TILSTAND", DataType::Utf8, true),  // Condition code
    ])
}

/// Create schema for standardized version of DODSAARSAG register data
#[must_use] pub fn dodsaarsag_standardized_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DEATH_CAUSE", DataType::Utf8, true),  // Normalized cause code
        Field::new("DEATH_CONDITION", DataType::Utf8, true),  // Normalized condition code
        Field::new("DEATH_CAUSE_CHAPTER", DataType::Utf8, true),  // ICD-10 chapter of death cause
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dodsaarsag_schema() {
        let schema = dodsaarsag_schema();
        
        // Verify PNR field
        let pnr_field = schema.field_with_name("PNR").unwrap();
        assert_eq!(pnr_field.data_type(), &DataType::Utf8);
        assert!(!pnr_field.is_nullable());
        
        // Verify C_AARSAG field
        let cause_field = schema.field_with_name("C_AARSAG").unwrap();
        assert_eq!(cause_field.data_type(), &DataType::Utf8);
        assert!(cause_field.is_nullable());
        
        // Verify C_TILSTAND field
        let condition_field = schema.field_with_name("C_TILSTAND").unwrap();
        assert_eq!(condition_field.data_type(), &DataType::Utf8);
        assert!(condition_field.is_nullable());
    }

    #[test]
    fn test_dodsaarsag_standardized_schema() {
        let schema = dodsaarsag_standardized_schema();
        
        // Verify PNR field
        let pnr_field = schema.field_with_name("PNR").unwrap();
        assert_eq!(pnr_field.data_type(), &DataType::Utf8);
        assert!(!pnr_field.is_nullable());
        
        // Verify DEATH_CAUSE field
        let cause_field = schema.field_with_name("DEATH_CAUSE").unwrap();
        assert_eq!(cause_field.data_type(), &DataType::Utf8);
        assert!(cause_field.is_nullable());
        
        // Verify DEATH_CONDITION field
        let condition_field = schema.field_with_name("DEATH_CONDITION").unwrap();
        assert_eq!(condition_field.data_type(), &DataType::Utf8);
        assert!(condition_field.is_nullable());
        
        // Verify DEATH_CAUSE_CHAPTER field
        let chapter_field = schema.field_with_name("DEATH_CAUSE_CHAPTER").unwrap();
        assert_eq!(chapter_field.data_type(), &DataType::Utf8);
        assert!(chapter_field.is_nullable());
    }
}