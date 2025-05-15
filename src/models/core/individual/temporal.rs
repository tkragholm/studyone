//! Temporal validity and age-related functionality
//!
//! This module contains implementations for time-dependent operations on Individuals,
//! such as age calculation, validity checking, and temporal snapshots.

use crate::models::core::individual::Individual;
use crate::models::core::traits::{HealthStatus, TemporalValidity};
use chrono::NaiveDate;

// Implement TemporalValidity trait for Individual
impl TemporalValidity for Individual {
    /// Check if this entity was valid at a specific date
    fn was_valid_at(&self, date: &NaiveDate) -> bool {
        // For individuals, we consider them valid if they were born before or on the date
        // and either they haven't died yet or they died after the date
        match self.birth_date {
            Some(birth) => {
                birth <= *date
                    && match self.death_date {
                        Some(death) => death >= *date,
                        None => true, // No death date means still alive
                    }
            }
            None => false, // No birth date means we can't determine validity
        }
    }

    /// Get the start date of validity (birth date)
    fn valid_from(&self) -> NaiveDate {
        // Return birth date or a default date if not available
        self.birth_date
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap())
    }

    /// Get the end date of validity (death date if any)
    fn valid_to(&self) -> Option<NaiveDate> {
        self.death_date
    }

    /// Create a snapshot of this entity at a specific point in time
    fn snapshot_at(&self, date: &NaiveDate) -> Option<Self> {
        if self.was_valid_at(date) {
            Some(self.clone())
        } else {
            None
        }
    }
}

// Additional temporal methods for Individual
impl Individual {
    /// Determine if this individual is a child based on age at reference date
    #[must_use]
    pub fn is_child(&self, reference_date: &NaiveDate) -> bool {
        if let Some(age) = self.age_at(reference_date) {
            age < 18
        } else {
            false
        }
    }

    /// Get the role of this individual at a reference date
    #[must_use]
    pub fn role_at(&self, reference_date: &NaiveDate, all_individuals: &[Self]) -> super::Role {
        let is_child = self.is_child(reference_date);
        let is_parent = self.is_parent_in_dataset(all_individuals);

        match (is_child, is_parent) {
            (true, true) => super::Role::ChildAndParent,
            (true, false) => super::Role::Child,
            (false, true) => super::Role::Parent,
            (false, false) => super::Role::Other,
        }
    }
}
