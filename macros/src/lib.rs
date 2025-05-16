//! Procedural macros for the par-reader crate
//!
//! This crate provides procedural macros for generating code from schema
//! definitions, significantly reducing boilerplate in the par-reader crate.

use proc_macro::TokenStream;

// Import modules
mod utils;
mod registry_trait;
mod property_field_impl;

// Tests
#[cfg(test)]
mod tests;

/// Derive macro for generating registry traits
///
/// This macro generates a registry trait implementation from a struct definition.
///
/// # Example with PNR as identifier
///
/// ```rust
/// #[derive(RegistryTrait)]
/// #[registry(name = "VNDS", description = "Migration registry", id_field = "pnr")]
/// struct VndsRegistry {
///     #[field(name = "PNR")]
///     pnr: String,
///
///     #[field(name = "INDUD_KODE")]
///     migration_code: Option<String>,
///
///     #[field(name = "HAEND_DATO")]
///     event_date: Option<chrono::NaiveDate>,
/// }
/// ```
///
/// # Example with RECNUM as identifier (for `LPR_DIAG`)
///
/// ```rust
/// #[derive(RegistryTrait)]
/// #[registry(name = "LPR_DIAG", description = "LPR Diagnosis registry", id_field = "record_number")]
/// struct LprDiagRegistry {
///     #[field(name = "RECNUM")]
///     record_number: Option<String>,
///
///     #[field(name = "C_DIAG")]
///     diagnosis_code: Option<String>,
///
///     #[field(name = "C_DIAGTYPE")]
///     diagnosis_type: Option<String>,
/// }
/// ```
#[proc_macro_derive(RegistryTrait, attributes(registry, field))]
pub fn derive_registry_trait(input: TokenStream) -> TokenStream {
    registry_trait::process_derive_registry_trait(input)
}

/// PropertyField derive macro
///
/// This macro generates property reflection code for a struct, allowing fields
/// to be accessed and modified through a string-based property interface.
///
/// # Example
///
/// ```rust
/// #[derive(PropertyField)]
/// struct Person {
///     #[property(name = "first_name")]
///     name: String,
///     
///     #[property(name = "birth_date", registry = "BEF")]
///     dob: Option<NaiveDate>,
/// }
/// ```
#[proc_macro_derive(PropertyField, attributes(property))]
pub fn derive_property_field(input: TokenStream) -> TokenStream {
    property_field_impl::process_derive_property_field(input)
}