//! VNDS registry trait-based deserializer (macro version)
//!
//! This module provides a macro-generated trait-based deserializer
//! for VNDS registry data, using the unified schema definition.

use crate::generate_trait_deserializer;
use crate::registry::vnds::schema::create_vnds_schema;

// Generate the trait deserializer from the unified schema
generate_trait_deserializer!(VndsTraitDeserializer, "VNDS", create_vnds_schema);
