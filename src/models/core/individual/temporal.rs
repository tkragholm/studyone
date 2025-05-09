//! Temporal validity and age-related functionality
//!
//! This module contains implementations for time-dependent operations on Individuals,
//! such as age calculation, validity checking, and temporal snapshots.

use crate::models::core::traits::{HealthStatus, TemporalValidity};
use crate::models::core::individual::Individual;
use chrono::{Datelike, NaiveDate};

// Implement TemporalValidity trait
impl TemporalValidity for Individual {
    fn was_valid_at(&self, date: &NaiveDate) -> bool {
        self.was_alive_at(date)
    }

    fn valid_from(&self) -> NaiveDate {
        self.birth_date
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap())
    }

    fn valid_to(&self) -> Option<NaiveDate> {
        self.death_date
    }

    fn snapshot_at(&self, date: &NaiveDate) -> Option<Self> {
        if self.was_valid_at(date) {
            Some(self.clone())
        } else {
            None
        }
    }
}

// Implement HealthStatus trait
impl HealthStatus for Individual {
    /// Calculate age of the individual at a specific reference date
    fn age_at(&self, reference_date: &NaiveDate) -> Option<i32> {
        match self.birth_date {
            Some(birth_date) => {
                // Check if the individual was alive at the reference date
                if let Some(death_date) = self.death_date {
                    if death_date < *reference_date {
                        return None;
                    }
                }

                let years = reference_date.year() - birth_date.year();
                // Adjust for birthday not yet reached in the reference year
                if reference_date.month() < birth_date.month()
                    || (reference_date.month() == birth_date.month()
                        && reference_date.day() < birth_date.day())
                {
                    Some(years - 1)
                } else {
                    Some(years)
                }
            }
            None => None,
        }
    }

    /// Check if the individual was alive at a specific date
    fn was_alive_at(&self, date: &NaiveDate) -> bool {
        // Check birth date (must be born before or on the date)
        if let Some(birth) = self.birth_date {
            if birth > *date {
                return false;
            }
        } else {
            // Unknown birth date, can't determine
            return false;
        }

        // Check death date (must not have died before the date)
        if let Some(death) = self.death_date {
            if death < *date {
                return false;
            }
        }

        true
    }

    /// Check if the individual was resident in Denmark at a specific date
    fn was_resident_at(&self, date: &NaiveDate) -> bool {
        // Must be alive to be resident
        if !self.was_alive_at(date) {
            return false;
        }

        // Check emigration status
        if let Some(emigration) = self.emigration_date {
            if emigration <= *date {
                // Check if they immigrated back after emigration
                if let Some(immigration) = self.immigration_date {
                    return immigration > emigration && immigration <= *date;
                }
                return false;
            }
        }

        // Either never emigrated or emigrated after the date
        true
    }
}

// Additional temporal methods for Individual
impl Individual {
    /// Determine if this individual is a child based on age at reference date
    #[must_use] pub fn is_child(&self, reference_date: &NaiveDate) -> bool {
        if let Some(age) = self.age_at(reference_date) {
            age < 18
        } else {
            false
        }
    }
    
    /// Get the role of this individual at a reference date
    #[must_use] pub fn role_at(&self, reference_date: &NaiveDate, all_individuals: &[Individual]) -> super::Role {
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