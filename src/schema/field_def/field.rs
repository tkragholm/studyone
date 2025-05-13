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
    #[must_use] pub const fn to_arrow_type(&self, _nullable: bool) -> DataType {
        match self {
            Self::PNR => DataType::Utf8,
            Self::String => DataType::Utf8,
            Self::Integer => DataType::Int32,
            Self::Decimal => DataType::Float64,
            Self::Date => DataType::Date32,
            Self::Time => DataType::Time32(arrow::datatypes::TimeUnit::Second),
            Self::Boolean => DataType::Boolean,
            Self::Category => DataType::Int32,
            Self::Other => DataType::Utf8,
        }
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PNR => write!(f, "PNR"),
            Self::String => write!(f, "String"),
            Self::Integer => write!(f, "Integer"),
            Self::Decimal => write!(f, "Decimal"),
            Self::Date => write!(f, "Date"),
            Self::Time => write!(f, "Time"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Category => write!(f, "Category"),
            Self::Other => write!(f, "Other"),
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
