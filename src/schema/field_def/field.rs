//! Field definition for the unified schema system
//!
//! This module defines the core field definition structures that will be used
//! to centralize registry field definitions.

use arrow::datatypes::{DataType, Field};
use std::fmt;

/// Represents the semantic type of a field
///
/// This enum standardizes the types across different registries, allowing
/// easier mapping and conversion between different representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    /// Personal identifier (PNR)
    PNR,
    /// Text value
    String,
    /// Integer value
    Integer,
    /// Decimal value
    Decimal,
    /// Date value
    Date,
    /// Time value (time of day)
    Time,
    /// Boolean value
    Boolean,
    /// Categorical value (like gender, socioeconomic status)
    Category,
    /// Other or unknown type
    Other,
}

impl FieldType {
    /// Convert to Arrow `DataType`
    ///
    /// Returns the most appropriate Arrow `DataType` for this field type
    #[must_use] pub fn to_arrow_type(&self, _nullable: bool) -> DataType {
        match self {
            FieldType::PNR => DataType::Utf8,
            FieldType::String => DataType::Utf8,
            FieldType::Integer => DataType::Int32,
            FieldType::Decimal => DataType::Float64,
            FieldType::Date => DataType::Date32,
            FieldType::Time => DataType::Time32(arrow::datatypes::TimeUnit::Second),
            FieldType::Boolean => DataType::Boolean,
            FieldType::Category => DataType::Int32,
            FieldType::Other => DataType::Utf8,
        }
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::PNR => write!(f, "PNR"),
            FieldType::String => write!(f, "String"),
            FieldType::Integer => write!(f, "Integer"),
            FieldType::Decimal => write!(f, "Decimal"),
            FieldType::Date => write!(f, "Date"),
            FieldType::Time => write!(f, "Time"),
            FieldType::Boolean => write!(f, "Boolean"),
            FieldType::Category => write!(f, "Category"),
            FieldType::Other => write!(f, "Other"),
        }
    }
}

/// A unified field definition for registry schemas
///
/// This structure provides a single source of truth for field definitions
/// across different registries.
#[derive(Debug, Clone)]
pub struct FieldDefinition {
    /// Name of the field in the registry
    pub name: String,
    /// Description of the field
    pub description: String,
    /// Semantic type of the field
    pub field_type: FieldType,
    /// Whether the field can be null
    pub nullable: bool,
    /// Alternative names for this field in different registries
    pub aliases: Vec<String>,
}

impl FieldDefinition {
    /// Create a new field definition
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        field_type: FieldType,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            field_type,
            nullable,
            aliases: Vec::new(),
        }
    }

    /// Add an alias for this field
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.aliases.push(alias.into());
        self
    }

    /// Add multiple aliases for this field
    #[must_use] pub fn with_aliases(mut self, aliases: Vec<impl Into<String>>) -> Self {
        self.aliases.extend(aliases.into_iter().map(std::convert::Into::into));
        self
    }

    /// Convert to an Arrow Field
    #[must_use] pub fn to_arrow_field(&self) -> Field {
        Field::new(
            &self.name,
            self.field_type.to_arrow_type(self.nullable),
            self.nullable,
        )
    }

    /// Check if the given name matches this field or any of its aliases
    #[must_use] pub fn matches_name(&self, name: &str) -> bool {
        if self.name == name {
            return true;
        }
        self.aliases.iter().any(|alias| alias == name)
    }
}
