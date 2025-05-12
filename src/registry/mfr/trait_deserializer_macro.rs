//! MFR registry trait-based deserializer (macro version)
//!
//! This module provides a macro-generated trait-based deserializer
//! for MFR registry data, using the unified schema definition.

use crate::generate_trait_deserializer;
use crate::registry::mfr::schema::create_mfr_schema;

// Generate the trait deserializer from the unified schema
generate_trait_deserializer!(MfrTraitDeserializer, "MFR", create_mfr_schema);
