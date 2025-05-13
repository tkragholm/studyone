//! Unified schema definition for DOD registry
//!
//! This module provides a unified schema for the DOD registry data
//! that maps Arrow record batch fields to Individual model fields
//! using trait-based field access.

use std::any::Any;
use std::sync::Arc;

use chrono::NaiveDate;

use crate::models::core::Individual;
use crate::models::core::registry_traits::DodFields;
use crate::schema::field_def::{Extractors, ModelSetters};
use crate::schema::field_def::{FieldDefinition, FieldMapping, FieldType};
use crate::schema::field_def::{RegistrySchema, create_registry_schema};

/// Create a closure for setting the PNR field on an Individual
fn pnr_setter() -> Arc<dyn Fn(&mut dyn Any, &dyn Any) + Send + Sync> {
    Arc::new(|target, value| {
        if let Some(individual) = target.downcast_mut::<Individual>() {
            if let Some(value) = value.downcast_ref::<String>() {
                individual.pnr = value.clone();
            }
        }
    })
}

/// Create a closure for setting the death date field on an Individual
fn death_date_setter() -> Arc<dyn Fn(&mut dyn Any, &dyn Any) + Send + Sync> {
    Arc::new(|target, value| {
        if let Some(individual) = target.downcast_mut::<Individual>() {
            if let Some(value) = value.downcast_ref::<NaiveDate>() {
                let dod_fields: &mut dyn DodFields = individual;
                dod_fields.set_death_date(Some(*value));
            }
        }
    })
}

/// Create a closure for setting the death cause field on an Individual
fn death_cause_setter() -> Arc<dyn Fn(&mut dyn Any, &dyn Any) + Send + Sync> {
    Arc::new(|target, value| {
        if let Some(individual) = target.downcast_mut::<Individual>() {
            if let Some(value) = value.downcast_ref::<String>() {
                let dod_fields: &mut dyn DodFields = individual;
                dod_fields.set_death_cause(Some(value.clone()));
            }
        }
    })
}

/// Create a closure for setting the underlying death cause field on an Individual
fn underlying_death_cause_setter() -> Arc<dyn Fn(&mut dyn Any, &dyn Any) + Send + Sync> {
    Arc::new(|target, value| {
        if let Some(individual) = target.downcast_mut::<Individual>() {
            if let Some(value) = value.downcast_ref::<String>() {
                let dod_fields: &mut dyn DodFields = individual;
                dod_fields.set_underlying_death_cause(Some(value.clone()));
            }
        }
    })
}

/// Get the unified schema for DOD registry
pub fn create_dod_schema() -> RegistrySchema {
    // Create field mappings
    let field_mappings = vec![
        // PNR field
        FieldMapping::new(
            FieldDefinition::new(
                "PNR",
                "Personal identification number",
                FieldType::PNR,
                false,
            ),
            Extractors::string("PNR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // Death date field
        FieldMapping::new(
            FieldDefinition::new("DODDATO", "Date of death", FieldType::Date, true),
            Extractors::date("DODDATO"),
            ModelSetters::date_setter(|individual, value| {
                let dod_fields: &mut dyn DodFields = individual;
                dod_fields.set_death_date(Some(value));
            }),
        ),
        // Death cause field (might be in separate batches)
        FieldMapping::new(
            FieldDefinition::new(
                "C_AARSAG",
                "Cause of death (ICD-10 code)",
                FieldType::String,
                true,
            ),
            Extractors::string("C_AARSAG"),
            ModelSetters::string_setter(|individual, value| {
                let dod_fields: &mut dyn DodFields = individual;
                dod_fields.set_death_cause(Some(value));
            }),
        ),
        // Underlying death cause field
        FieldMapping::new(
            FieldDefinition::new(
                "C_TILSTAND",
                "Underlying cause of death",
                FieldType::String,
                true,
            ),
            Extractors::string("C_TILSTAND"),
            ModelSetters::string_setter(|individual, value| {
                let dod_fields: &mut dyn DodFields = individual;
                dod_fields.set_underlying_death_cause(Some(value));
            }),
        ),
    ];

    // Create the registry schema
    create_registry_schema(
        "DOD",
        "Death registry (Causes and dates of death)",
        field_mappings,
    )
}
