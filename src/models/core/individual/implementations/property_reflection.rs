//! Property reflection system for Individual model
//!
//! This module defines traits and implementations for reflecting property values
//! between dedicated fields and the dynamic properties map. This system makes
//! property handling more robust and reduces code duplication.

use chrono::NaiveDate;
use std::any::Any;
use std::collections::HashMap;

use crate::models::core::Individual;

/// Trait for reflecting properties between dedicated fields and the properties map
pub trait PropertyReflection {
    /// Set a property, handling both dedicated field and properties map
    fn set_reflected_property(&mut self, property: &str, value: Box<dyn Any + Send + Sync>);

    /// Get a property, checking both dedicated field and properties map
    fn get_reflected_property(&self, property: &str) -> Option<Box<dyn Any + Send + Sync>>;

    /// Store a property in the properties map
    fn store_property(&mut self, property: &str, value: Box<dyn Any + Send + Sync>);

    /// Set a string property
    fn set_string_property(
        &mut self,
        property: &str,
        field: &mut String,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Set an optional string property
    fn set_option_string_property(
        &mut self,
        property: &str,
        field: &mut Option<String>,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Set an optional date property
    fn set_option_date_property(
        &mut self,
        property: &str,
        field: &mut Option<NaiveDate>,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Set an optional integer property
    fn set_option_i32_property(
        &mut self,
        property: &str,
        field: &mut Option<i32>,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Set an optional float property
    fn set_option_f64_property(
        &mut self,
        property: &str,
        field: &mut Option<f64>,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Set an optional string array property
    fn set_option_string_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<String>>,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Set an optional date array property
    fn set_option_date_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<NaiveDate>>,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Add to a string array property
    fn add_to_string_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<String>>,
        value: Box<dyn Any + Send + Sync>,
    );

    /// Add to a date array property
    fn add_to_date_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<NaiveDate>>,
        value: Box<dyn Any + Send + Sync>,
    );
}

impl PropertyReflection for Individual {
    fn set_reflected_property(&mut self, property: &str, value: Box<dyn Any + Send + Sync>) {
        // First try to set using the generated code from the PropertyField macro
        // We can't clone Box<dyn Any>, so we need to use reference instead
        self.set_property_field(property, value);
        
        // Handle special cases that need custom handling (original value now consumed)
        {}
    }

    fn get_reflected_property(&self, property: &str) -> Option<Box<dyn Any + Send + Sync>> {
        // First check if it's a dedicated field using the reflective approach
        let value: Option<Box<dyn Any + Send + Sync>> = match property {
            "pnr" => Some(Box::new(self.pnr.clone())),
            "gender" => Some(Box::new(self.gender.clone())),
            "birth_date" => Some(Box::new(self.birth_date)),
            "death_date" => Some(Box::new(self.death_date)),
            "mother_pnr" => Some(Box::new(self.mother_pnr.clone())),
            "father_pnr" => Some(Box::new(self.father_pnr.clone())),
            "family_id" => Some(Box::new(self.family_id.clone())),
            "spouse_pnr" => Some(Box::new(self.spouse_pnr.clone())),
            "family_size" => Some(Box::new(self.family_size)),
            "family_type" => Some(Box::new(self.family_type)),
            "position_in_family" => Some(Box::new(self.position_in_family)),
            "residence_from" => Some(Box::new(self.residence_from)),
            "marital_date" => Some(Box::new(self.marital_date)),
            "event_date" => Some(Box::new(self.event_date)),
            "event_type" => Some(Box::new(self.event_type.clone())),
            "origin" => Some(Box::new(self.origin.clone())),
            "citizenship_status" => Some(Box::new(self.citizenship_status.clone())),
            "immigration_type" => Some(Box::new(self.immigration_type.clone())),
            "marital_status" => Some(Box::new(self.marital_status.clone())),
            "municipality_code" => Some(Box::new(self.municipality_code.clone())),
            "regional_code" => Some(Box::new(self.regional_code.clone())),
            "education_code" => Some(Box::new(self.education_code)),
            "education_valid_from" => Some(Box::new(self.education_valid_from)),
            "education_valid_to" => Some(Box::new(self.education_valid_to)),
            "education_institution" => Some(Box::new(self.education_institution)),
            "socioeconomic_status" => Some(Box::new(self.socioeconomic_status)),
            "annual_income" => Some(Box::new(self.annual_income)),
            "employment_income" => Some(Box::new(self.employment_income)),
            "income_year" => Some(Box::new(self.income_year)),
            "hospital_admissions_count" => Some(Box::new(self.hospital_admissions_count)),
            "emergency_visits_count" => Some(Box::new(self.emergency_visits_count)),
            "outpatient_visits_count" => Some(Box::new(self.outpatient_visits_count)),
            "gp_visits_count" => Some(Box::new(self.gp_visits_count)),
            "hospitalization_days" => Some(Box::new(self.hospitalization_days)),
            "length_of_stay" => Some(Box::new(self.length_of_stay)),
            "last_hospital_admission_date" => Some(Box::new(self.last_hospital_admission_date)),
            "diagnoses" => Some(Box::new(self.diagnoses.clone())),
            "procedures" => Some(Box::new(self.procedures.clone())),
            "hospital_admissions" => Some(Box::new(self.hospital_admissions.clone())),
            "discharge_dates" => Some(Box::new(self.discharge_dates.clone())),
            "death_cause" => Some(Box::new(self.death_cause.clone())),
            "underlying_death_cause" => Some(Box::new(self.underlying_death_cause.clone())),
            "birth_weight" => Some(Box::new(self.birth_weight)),
            "birth_length" => Some(Box::new(self.birth_length)),
            "gestational_age" => Some(Box::new(self.gestational_age)),
            "apgar_score" => Some(Box::new(self.apgar_score)),
            "birth_order" => Some(Box::new(self.birth_order)),
            "plurality" => Some(Box::new(self.plurality)),
            "household_size" => Some(Box::new(self.household_size)),
            "age" => Some(Box::new(self.age)),

            // For other fields, check the properties map
            _ => {
                if let Some(ref props) = self.properties {
                    if let Some(value) = props.get(property) {
                        // Need to clone the boxed value to return it safely
                        // We can't just clone value directly since Box<dyn Any> doesn't implement Clone
                        let boxed_clone: Box<dyn Any + Send + Sync> = match property {
                            // Handle common types that we know how to clone
                            _ if value.is::<String>() => Box::new(value.downcast_ref::<String>().unwrap().clone()),
                            _ if value.is::<Option<String>>() => Box::new(value.downcast_ref::<Option<String>>().unwrap().clone()),
                            _ if value.is::<Option<NaiveDate>>() => Box::new(*value.downcast_ref::<Option<NaiveDate>>().unwrap()),
                            _ if value.is::<Option<i32>>() => Box::new(*value.downcast_ref::<Option<i32>>().unwrap()),
                            _ if value.is::<Option<f64>>() => Box::new(*value.downcast_ref::<Option<f64>>().unwrap()),
                            _ if value.is::<Option<Vec<String>>>() => Box::new(value.downcast_ref::<Option<Vec<String>>>().unwrap().clone()),
                            _ if value.is::<Option<Vec<NaiveDate>>>() => Box::new(value.downcast_ref::<Option<Vec<NaiveDate>>>().unwrap().clone()),
                            // Default case - unknown type that we can't safely clone
                            _ => return None,
                        };
                        return Some(boxed_clone);
                    }
                }
                None
            }
        };

        // If it's a dedicated field, return its value
        if let Some(val) = value {
            return Some(val);
        }

        // If not found, check the properties map
        if let Some(ref props) = self.properties {
            if let Some(value) = props.get(property) {
                // Need to clone the boxed value to return it safely
                // We can't just clone value directly since Box<dyn Any> doesn't implement Clone
                let boxed_clone: Box<dyn Any + Send + Sync> = match property {
                    // Handle common types that we know how to clone
                    _ if value.is::<String>() => Box::new(value.downcast_ref::<String>().unwrap().clone()),
                    _ if value.is::<Option<String>>() => Box::new(value.downcast_ref::<Option<String>>().unwrap().clone()),
                    _ if value.is::<Option<NaiveDate>>() => Box::new(*value.downcast_ref::<Option<NaiveDate>>().unwrap()),
                    _ if value.is::<Option<i32>>() => Box::new(*value.downcast_ref::<Option<i32>>().unwrap()),
                    _ if value.is::<Option<f64>>() => Box::new(*value.downcast_ref::<Option<f64>>().unwrap()),
                    _ if value.is::<Option<Vec<String>>>() => Box::new(value.downcast_ref::<Option<Vec<String>>>().unwrap().clone()),
                    _ if value.is::<Option<Vec<NaiveDate>>>() => Box::new(value.downcast_ref::<Option<Vec<NaiveDate>>>().unwrap().clone()),
                    // Default case - unknown type that we can't safely clone
                    _ => return None,
                };
                return Some(boxed_clone);
            }
        }

        None
    }

    fn store_property(&mut self, property: &str, value: Box<dyn Any + Send + Sync>) {
        // Create the properties map if it doesn't exist
        if self.properties.is_none() {
            self.properties = Some(HashMap::new());
        }

        // Now we can safely unwrap and insert
        if let Some(props) = &mut self.properties {
            props.insert(property.to_string(), value);
        }
    }

    fn set_string_property(
        &mut self,
        property: &str,
        field: &mut String,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<String>() {
            *field = v.clone();
        }
        self.store_property(property, value);
    }

    fn set_option_string_property(
        &mut self,
        property: &str,
        field: &mut Option<String>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<String>>() {
            *field = v.clone();
        }
        self.store_property(property, value);
    }

    fn set_option_date_property(
        &mut self,
        property: &str,
        field: &mut Option<NaiveDate>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
            *field = *v;
        }
        self.store_property(property, value);
    }

    fn set_option_i32_property(
        &mut self,
        property: &str,
        field: &mut Option<i32>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<i32>>() {
            *field = *v;
        }
        self.store_property(property, value);
    }

    fn set_option_f64_property(
        &mut self,
        property: &str,
        field: &mut Option<f64>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<f64>>() {
            *field = *v;
        }
        self.store_property(property, value);
    }

    fn set_option_string_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<String>>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<Vec<String>>>() {
            *field = v.clone();
        }
        self.store_property(property, value);
    }

    fn set_option_date_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<NaiveDate>>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<Vec<NaiveDate>>>() {
            *field = v.clone();
        }
        self.store_property(property, value);
    }

    fn add_to_string_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<String>>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<String>>() {
            if let Some(string_value) = v {
                if let Some(vec) = field {
                    vec.push(string_value.clone());
                } else {
                    *field = Some(vec![string_value.clone()]);
                }
            }
        }
        self.store_property(property, value);
    }

    fn add_to_date_vec_property(
        &mut self,
        property: &str,
        field: &mut Option<Vec<NaiveDate>>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
            if let Some(date) = *v {
                if let Some(vec) = field {
                    vec.push(date);
                } else {
                    *field = Some(vec![date]);
                }
            }
        }
        self.store_property(property, value);
    }
}