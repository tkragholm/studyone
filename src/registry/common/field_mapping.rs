//! Common field mappings for registry deserialization
//!
//! This module provides common field mappings that can be used across different registries.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create a default set of field mappings for any registry type
///
/// This provides basic mappings that should work with most registry types.
#[must_use] pub fn create_default_field_mappings() -> Vec<FieldMapping> {
    vec![
        // PNR mapping (required for most registries)
        FieldMapping::new(
            FieldDefinition::new("PNR", "pnr", FieldType::PNR, false),
            Extractors::string("PNR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
    ]
}
