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
        // First set the value in dedicated fields when appropriate using match statement
        match property {
            "pnr" => {
                if let Some(v) = value.downcast_ref::<String>() {
                    self.pnr = v.clone();
                }
                self.store_property(property, value);
            }
            "gender" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.gender = v.clone();
                }
                self.store_property(property, value);
            }
            "birth_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.birth_date = *v;
                }
                self.store_property(property, value);
            }
            "death_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.death_date = *v;
                }
                self.store_property(property, value);
            }
            "mother_pnr" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.mother_pnr = v.clone();
                }
                self.store_property(property, value);
            }
            "father_pnr" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.father_pnr = v.clone();
                }
                self.store_property(property, value);
            }
            "family_id" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.family_id = v.clone();
                }
                self.store_property(property, value);
            }
            "spouse_pnr" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.spouse_pnr = v.clone();
                }
                self.store_property(property, value);
            }
            "family_size" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.family_size = *v;
                }
                self.store_property(property, value);
            }
            "family_type" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.family_type = *v;
                }
                self.store_property(property, value);
            }
            "position_in_family" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.position_in_family = *v;
                }
                self.store_property(property, value);
            }
            "residence_from" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.residence_from = *v;
                }
                self.store_property(property, value);
            }
            "marital_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.marital_date = *v;
                }
                self.store_property(property, value);
            }
            "event_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.event_date = *v;
                }
                self.store_property(property, value);
            }
            "event_type" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.event_type = v.clone();
                }
                self.store_property(property, value);
            }
            "origin" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.origin = v.clone();
                }
                self.store_property(property, value);
            }
            "citizenship_status" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.citizenship_status = v.clone();
                }
                self.store_property(property, value);
            }
            "immigration_type" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.immigration_type = v.clone();
                }
                self.store_property(property, value);
            }
            "marital_status" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.marital_status = v.clone();
                }
                self.store_property(property, value);
            }
            "municipality_code" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.municipality_code = v.clone();
                }
                self.store_property(property, value);
            }
            "regional_code" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.regional_code = v.clone();
                }
                self.store_property(property, value);
            }
            "education_code" => {
                if let Some(v) = value.downcast_ref::<Option<u16>>() {
                    self.education_code = *v;
                }
                self.store_property(property, value);
            }
            "education_valid_from" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.education_valid_from = *v;
                }
                self.store_property(property, value);
            }
            "education_valid_to" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.education_valid_to = *v;
                }
                self.store_property(property, value);
            }
            "education_institution" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.education_institution = *v;
                }
                self.store_property(property, value);
            }
            "socioeconomic_status" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.socioeconomic_status = *v;
                }
                self.store_property(property, value);
            }
            "annual_income" => {
                if let Some(v) = value.downcast_ref::<Option<f64>>() {
                    self.annual_income = *v;
                }
                self.store_property(property, value);
            }
            "employment_income" => {
                if let Some(v) = value.downcast_ref::<Option<f64>>() {
                    self.employment_income = *v;
                }
                self.store_property(property, value);
            }
            "income_year" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.income_year = *v;
                }
                self.store_property(property, value);
            }
            "hospital_admissions_count" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.hospital_admissions_count = *v;
                }
                self.store_property(property, value);
            }
            "emergency_visits_count" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.emergency_visits_count = *v;
                }
                self.store_property(property, value);
            }
            "outpatient_visits_count" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.outpatient_visits_count = *v;
                }
                self.store_property(property, value);
            }
            "gp_visits_count" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.gp_visits_count = *v;
                }
                self.store_property(property, value);
            }
            "hospitalization_days" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.hospitalization_days = *v;
                }
                self.store_property(property, value);
            }
            "length_of_stay" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.length_of_stay = *v;
                }
                self.store_property(property, value);
            }
            "last_hospital_admission_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.last_hospital_admission_date = *v;
                }
                self.store_property(property, value);
            }
            "diagnoses" => {
                if let Some(v) = value.downcast_ref::<Option<Vec<String>>>() {
                    self.diagnoses = v.clone();
                }
                self.store_property(property, value);
            }
            "procedures" => {
                if let Some(v) = value.downcast_ref::<Option<Vec<String>>>() {
                    self.procedures = v.clone();
                }
                self.store_property(property, value);
            }
            "hospital_admissions" => {
                if let Some(v) = value.downcast_ref::<Option<Vec<NaiveDate>>>() {
                    self.hospital_admissions = v.clone();
                }
                self.store_property(property, value);
            }
            "discharge_dates" => {
                if let Some(v) = value.downcast_ref::<Option<Vec<NaiveDate>>>() {
                    self.discharge_dates = v.clone();
                }
                self.store_property(property, value);
            }
            "death_cause" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.death_cause = v.clone();
                }
                self.store_property(property, value);
            }
            "underlying_death_cause" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.underlying_death_cause = v.clone();
                }
                self.store_property(property, value);
            }
            "birth_weight" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.birth_weight = *v;
                }
                self.store_property(property, value);
            }
            "birth_length" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.birth_length = *v;
                }
                self.store_property(property, value);
            }
            "gestational_age" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.gestational_age = *v;
                }
                self.store_property(property, value);
            }
            "apgar_score" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.apgar_score = *v;
                }
                self.store_property(property, value);
            }
            "birth_order" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.birth_order = *v;
                }
                self.store_property(property, value);
            }
            "plurality" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.plurality = *v;
                }
                self.store_property(property, value);
            }

            // Special handling for array additions
            "action_diagnosis" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    if let Some(string_value) = v {
                        if let Some(ref mut diagnoses) = self.diagnoses {
                            diagnoses.push(string_value.clone());
                        } else {
                            self.diagnoses = Some(vec![string_value.clone()]);
                        }
                    }
                }
                self.store_property(property, value);
            }
            "diagnosis_code" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    if let Some(string_value) = v {
                        if let Some(ref mut diagnoses) = self.diagnoses {
                            diagnoses.push(string_value.clone());
                        } else {
                            self.diagnoses = Some(vec![string_value.clone()]);
                        }
                    }
                }
                self.store_property(property, value);
            }
            "admission_date" => {
                // Handle admission date specially because it affects multiple fields
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    if let Some(date) = *v {
                        // Add to hospital_admissions array
                        if let Some(ref mut admissions) = self.hospital_admissions {
                            admissions.push(date);
                        } else {
                            self.hospital_admissions = Some(vec![date]);
                        }

                        // Update last_hospital_admission_date if needed
                        if let Some(last_date) = self.last_hospital_admission_date {
                            if date > last_date {
                                self.last_hospital_admission_date = Some(date);
                            }
                        } else {
                            self.last_hospital_admission_date = Some(date);
                        }
                    }
                }
                self.store_property(property, value);
            }
            "discharge_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    if let Some(date) = *v {
                        if let Some(ref mut dates) = self.discharge_dates {
                            dates.push(date);
                        } else {
                            self.discharge_dates = Some(vec![date]);
                        }
                    }
                }
                self.store_property(property, value);
            }
            "age" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.age = *v;
                }
                self.store_property(property, value);
            }

            // Other fields are just stored in the properties map
            _ => self.store_property(property, value),
        }
    }

    fn get_reflected_property(&self, property: &str) -> Option<Box<dyn Any + Send + Sync>> {
        // First check if it's a dedicated field
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