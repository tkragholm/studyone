//! Registry trait implementations for Individual
//!
//! This module implements various registry-specific traits for the Individual model,
//! defining how registry data is accessed and manipulated.

use crate::models::core::individual::consolidated::Individual;
use crate::models::core::registry_traits::*;
use chrono::NaiveDate;

// Implement the BefFields trait for Individual
impl BefFields for Individual {
    fn spouse_pnr(&self) -> Option<&str> {
        self.spouse_pnr.as_deref()
    }

    fn set_spouse_pnr(&mut self, value: Option<String>) {
        self.spouse_pnr = value;
    }

    fn family_size(&self) -> Option<i32> {
        self.family_size
    }

    fn set_family_size(&mut self, value: Option<i32>) {
        self.family_size = value;
    }

    fn residence_from(&self) -> Option<NaiveDate> {
        self.residence_from
    }

    fn set_residence_from(&mut self, value: Option<NaiveDate>) {
        self.residence_from = value;
    }

    fn migration_type(&self) -> Option<&str> {
        self.event_type.as_deref()
    }

    fn set_migration_type(&mut self, value: Option<String>) {
        self.event_type = value;
    }

    fn position_in_family(&self) -> Option<i32> {
        self.position_in_family
    }

    fn set_position_in_family(&mut self, value: Option<i32>) {
        self.position_in_family = value;
    }

    fn family_type(&self) -> Option<i32> {
        self.family_type
    }

    fn set_family_type(&mut self, value: Option<i32>) {
        self.family_type = value;
    }
}
