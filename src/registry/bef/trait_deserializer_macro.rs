//! BEF registry trait-based deserializer (macro version)
//!
//! This module provides a macro-generated trait-based deserializer
//! for BEF registry data, using the unified schema definition.

use crate::generate_trait_deserializer;
use crate::registry::bef::schema::create_bef_schema;

// Generate the trait deserializer from the unified schema
generate_trait_deserializer!(BefTraitDeserializer, "BEF", create_bef_schema);
