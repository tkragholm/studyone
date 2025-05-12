//! UDDF registry trait-based deserializer (macro version)
//!
//! This module provides a macro-generated trait-based deserializer
//! for UDDF registry data, using the unified schema definition.

use crate::generate_trait_deserializer;
use crate::registry::uddf::schema::create_uddf_schema;

// Generate the trait deserializer from the unified schema
generate_trait_deserializer!(UddfTraitDeserializer, "UDDF", create_uddf_schema);
