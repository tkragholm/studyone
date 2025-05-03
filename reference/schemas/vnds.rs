//! Schema for the Danish Migration Register (VNDS)
//!
//! This module defines the schema for the Danish Migration Register,
//! which contains information about migration in and out of Denmark.

use arrow::datatypes::{DataType, Field, Schema};

/// Create the schema for the Danish Migration Register (VNDS)
#[must_use] pub fn vnds_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("INDUD_KODE", DataType::Utf8, true),  // Migration code (in/out)
        Field::new("HAEND_DATO", DataType::Utf8, true),  // Event date
    ])
}

/// Create schema for standardized version of VNDS register data
#[must_use] pub fn vnds_standardized_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("MIGRATION_TYPE", DataType::Utf8, true),  // "IN" or "OUT"
        Field::new("MIGRATION_DATE", DataType::Date32, true),  // Standardized date
    ])
}

/// Migration type values
pub enum MigrationType {
    /// Immigration into Denmark
    Immigration,
    /// Emigration out of Denmark
    Emigration,
}

impl MigrationType {
    /// Convert a migration code to a migration type
    #[must_use] pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "I" | "i" | "1" => Some(Self::Immigration),
            "U" | "u" | "0" => Some(Self::Emigration),
            _ => None,
        }
    }
    
    /// Get the string representation of the migration type
    #[must_use] pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Immigration => "IN",
            Self::Emigration => "OUT",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vnds_schema() {
        let schema = vnds_schema();
        
        // Verify PNR field
        let pnr_field = schema.field_with_name("PNR").unwrap();
        assert_eq!(pnr_field.data_type(), &DataType::Utf8);
        assert!(!pnr_field.is_nullable());
        
        // Verify INDUD_KODE field
        let code_field = schema.field_with_name("INDUD_KODE").unwrap();
        assert_eq!(code_field.data_type(), &DataType::Utf8);
        assert!(code_field.is_nullable());
        
        // Verify HAEND_DATO field
        let date_field = schema.field_with_name("HAEND_DATO").unwrap();
        assert_eq!(date_field.data_type(), &DataType::Utf8);
        assert!(date_field.is_nullable());
    }

    #[test]
    fn test_vnds_standardized_schema() {
        let schema = vnds_standardized_schema();
        
        // Verify PNR field
        let pnr_field = schema.field_with_name("PNR").unwrap();
        assert_eq!(pnr_field.data_type(), &DataType::Utf8);
        assert!(!pnr_field.is_nullable());
        
        // Verify MIGRATION_TYPE field
        let type_field = schema.field_with_name("MIGRATION_TYPE").unwrap();
        assert_eq!(type_field.data_type(), &DataType::Utf8);
        assert!(type_field.is_nullable());
        
        // Verify MIGRATION_DATE field
        let date_field = schema.field_with_name("MIGRATION_DATE").unwrap();
        assert_eq!(date_field.data_type(), &DataType::Date32);
        assert!(date_field.is_nullable());
    }
    
    #[test]
    fn test_migration_type_conversion() {
        // Test immigration codes
        assert!(matches!(MigrationType::from_code("I").unwrap(), MigrationType::Immigration));
        assert!(matches!(MigrationType::from_code("i").unwrap(), MigrationType::Immigration));
        assert!(matches!(MigrationType::from_code("1").unwrap(), MigrationType::Immigration));
        
        // Test emigration codes
        assert!(matches!(MigrationType::from_code("U").unwrap(), MigrationType::Emigration));
        assert!(matches!(MigrationType::from_code("u").unwrap(), MigrationType::Emigration));
        assert!(matches!(MigrationType::from_code("0").unwrap(), MigrationType::Emigration));
        
        // Test invalid code
        assert!(MigrationType::from_code("X").is_none());
    }
    
    #[test]
    fn test_migration_type_as_str() {
        assert_eq!(MigrationType::Immigration.as_str(), "IN");
        assert_eq!(MigrationType::Emigration.as_str(), "OUT");
    }
}