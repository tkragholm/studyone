//! IND registry trait-based deserializer (macro version)
//!
//! This module provides a macro-generated trait-based deserializer
//! for IND registry data, using the unified schema definition.

use crate::generate_trait_deserializer;
use crate::registry::ind::schema::create_ind_schema;

// Generate the trait deserializer from the unified schema
generate_trait_deserializer!(IndTraitDeserializer, "IND", create_ind_schema);
