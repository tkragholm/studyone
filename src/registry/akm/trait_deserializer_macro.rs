//! AKM registry trait-based deserializer (macro version)
//!
//! This module provides a macro-generated trait-based deserializer
//! for AKM registry data, using the unified schema definition.

use crate::generate_trait_deserializer;
use crate::registry::akm::schema::create_akm_schema;

// Generate the trait deserializer from the unified schema
generate_trait_deserializer!(AkmTraitDeserializer, "AKM", create_akm_schema);
