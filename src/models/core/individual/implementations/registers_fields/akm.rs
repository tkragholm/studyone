//! Registry trait implementations for Individual
//!
//! This module implements various registry-specific traits for the Individual model,
//! defining how registry data is accessed and manipulated.

use crate::models::core::individual::consolidated::Individual;
use crate::models::core::registry_traits::*;
use chrono::NaiveDate;

// Implement AkmFields for Individual
impl AkmFields for Individual {
    fn occupation_code(&self) -> Option<&str> {
        // Look for occupation code in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("occupation_code") {
                if let Some(s) = cached.downcast_ref::<String>() {
                    return Some(s.as_str());
                }
            }
        }
        None
    }

    fn set_occupation_code(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.set_property("occupation_code", Box::new(v));
        }
    }

    fn industry_code(&self) -> Option<&str> {
        // Look for industry code in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("industry_code") {
                if let Some(s) = cached.downcast_ref::<String>() {
                    return Some(s.as_str());
                }
            }
        }
        None
    }

    fn set_industry_code(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.set_property("industry_code", Box::new(v));
        }
    }

    fn employment_start_date(&self) -> Option<NaiveDate> {
        // Look for employment start date in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("employment_start_date") {
                if let Some(date) = cached.downcast_ref::<Option<NaiveDate>>() {
                    return *date;
                }
            }
        }
        None
    }

    fn set_employment_start_date(&mut self, value: Option<NaiveDate>) {
        self.set_property("employment_start_date", Box::new(value));
    }

    fn employment_end_date(&self) -> Option<NaiveDate> {
        // Look for employment end date in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("employment_end_date") {
                if let Some(date) = cached.downcast_ref::<Option<NaiveDate>>() {
                    return *date;
                }
            }
        }
        None
    }

    fn set_employment_end_date(&mut self, value: Option<NaiveDate>) {
        self.set_property("employment_end_date", Box::new(value));
    }

    fn workplace_id(&self) -> Option<&str> {
        // Look for workplace ID in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("workplace_id") {
                if let Some(s) = cached.downcast_ref::<String>() {
                    return Some(s.as_str());
                }
            }
        }
        None
    }

    fn set_workplace_id(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.set_property("workplace_id", Box::new(v));
        }
    }

    fn working_hours(&self) -> Option<f64> {
        // Look for working hours in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("working_hours") {
                if let Some(hours) = cached.downcast_ref::<Option<f64>>() {
                    return *hours;
                }
            }
        }
        None
    }

    fn set_working_hours(&mut self, value: Option<f64>) {
        self.set_property("working_hours", Box::new(value));
    }
}
