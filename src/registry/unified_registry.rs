//! Utilities for implementing the unified schema system
//!
//! This module provides utility functions and traits for implementing
//! the unified schema system in registry loaders.

// Removed unused import
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

/// Trait for registries that support the unified schema system
pub trait UnifiedRegistrySupport {
    /// Get the schema using the unified system
    fn get_unified_schema(&self) -> SchemaRef;

    /// Get the schema using the original system
    fn get_original_schema(&self) -> SchemaRef;
}

/// Create a PNR filterable loader for a registry
///
/// This function creates a new PNR filterable loader for a registry,
/// using either the unified schema system or the original schema system.
#[must_use]
pub fn create_loader(
    unified_schema: SchemaRef,
    original_schema: SchemaRef,
    use_unified: bool,
    pnr_column: String, // Use owned String to avoid lifetime issues
) -> (SchemaRef, Arc<crate::async_io::Loader>) {
    let schema = if use_unified {
        unified_schema
    } else {
        original_schema
    };

    // Create the loader with specific implementation - we use concrete type instead of trait
    // Pass pnr_column directly since with_pnr_column takes ownership through impl Into<String>
    let loader = Arc::new(
        crate::async_io::Loader::with_schema_ref(schema.clone()).with_pnr_column(pnr_column),
    );

    (schema, loader)
}

/// Base implementation for registry loaders that support the unified system
///
/// This trait provides default implementations for the `use_unified_system` and
/// `is_unified_system_enabled` methods of the `RegisterLoader` trait.
pub trait UnifiedSystemSupport {
    /// Get the unified system flag
    fn get_unified_system_flag(&self) -> bool;

    /// Set the unified system flag
    fn set_unified_system_flag(&mut self, enable: bool);

    /// Update the schema based on the unified system setting
    fn update_schema(&mut self, enable: bool);

    /// Enable or disable the unified schema system
    fn use_unified_system(&mut self, enable: bool) {
        self.set_unified_system_flag(enable);
        self.update_schema(enable);
    }

    /// Check if the unified schema system is enabled
    fn is_unified_system_enabled(&self) -> bool {
        self.get_unified_system_flag()
    }
}
