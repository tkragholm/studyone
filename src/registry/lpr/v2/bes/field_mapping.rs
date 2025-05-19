//! Field mappings for LPR v2 BES registry deserialization
//!
//! This module defines the field mappings for the LPR v2 BES (Outpatient Visits) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for LPR v2 BES registry
#[must_use]
pub fn create_field_mappings() -> Vec<FieldMapping> {
    vec![
        // Record number (required, used as ID)
        FieldMapping::new(
            FieldDefinition::new("RECNUM", "record_number", FieldType::String, false),
            Extractors::string("RECNUM"),
            ModelSetters::string_setter(|individual, value| {
                // Store record number in properties map
                let record_num = value.as_str();
                {
                    individual
                        .store_property("lpr_bes_record_number", Box::new(record_num.to_string()));
                }
            }),
        ),
        // Outpatient visit date
        FieldMapping::new(
            FieldDefinition::new("D_AMBDTO", "outpatient_visit_date", FieldType::Date, true),
            Extractors::date("D_AMBDTO"),
            ModelSetters::date_setter(|individual, value| {
                // Increment outpatient visits count
                let value = value;
                let current_count = individual.outpatient_visits_count.unwrap_or(0);
                individual.outpatient_visits_count = Some(current_count + 1);

                // Store the outpatient visit date in properties map
                let date = value;
                // Create an array in properties map if needed
                let prop_name = "outpatient_visit_dates";
                let dates = if let Some(props) = &mut individual.properties {
                    if let Some(existing) = props.get_mut(prop_name) {
                        if let Some(dates) = existing.downcast_mut::<Vec<chrono::NaiveDate>>() {
                            dates.push(date);
                            None // We've already updated the existing vector
                        } else {
                            Some(vec![date]) // Wrong type, create new
                        }
                    } else {
                        Some(vec![date]) // Not found, create new
                    }
                } else {
                    // No properties map yet, create one
                    individual.properties = Some(std::collections::HashMap::new());
                    Some(vec![date])
                };

                // If we created a new vector, store it
                if let Some(new_dates) = dates {
                    if let Some(props) = &mut individual.properties {
                        props.insert(prop_name.to_string(), Box::new(new_dates));
                    }
                }
            }),
        ),
        // Delivery date
        FieldMapping::new(
            FieldDefinition::new("LEVERANCEDATO", "delivery_date", FieldType::Date, true),
            Extractors::date("LEVERANCEDATO"),
            ModelSetters::string_setter(|individual, value| {
                // Store the delivery date in properties map
                let date = value;
                individual.store_property("lpr_bes_delivery_date", Box::new(date));
            }),
        ),
        // Version
        FieldMapping::new(
            FieldDefinition::new("VERSION", "version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|individual, value| {
                // Store version in properties map
                let version = value.as_str();
                individual.store_property("lpr_bes_version", Box::new(version.to_string()));
            }),
        ),
    ]
}
