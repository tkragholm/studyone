//! Health validity and age-related functionality
//!
//! This module contains implementations for time-dependent operations on Individuals,
//! such as age calculation, validity checking, and temporal snapshots.

use crate::models::core::individual::Individual;
use crate::models::core::traits::HealthStatus;
use chrono::{Datelike, NaiveDate};

// Implement HealthStatus trait for Individual
impl HealthStatus for Individual {
    /// Check if the individual was alive at a specific date
    fn was_alive_at(&self, date: &NaiveDate) -> bool {
        // Same logic as was_valid_at
        match self.birth_date {
            Some(birth) => {
                birth <= *date
                    && match self.death_date {
                        Some(death) => death >= *date,
                        None => true, // No death date means still alive
                    }
            }
            None => false, // No birth date means we can't determine if alive
        }
    }

    /// Check if the individual was resident in Denmark at a specific date
    fn was_resident_at(&self, date: &NaiveDate) -> bool {
        // For simplicity, we assume residency if the individual was alive
        // A more accurate implementation would check migration events
        self.was_alive_at(date)
    }

    /// Calculate age at a specific reference date
    fn age_at(&self, reference_date: &NaiveDate) -> Option<i32> {
        match self.birth_date {
            Some(birth_date) => {
                if self.was_alive_at(reference_date) {
                    // Calculate years between birth date and reference date
                    let years = reference_date.year() - birth_date.year();

                    // Adjust for month and day (if birthday hasn't occurred yet this year)
                    let adjustment = if reference_date.month() < birth_date.month()
                        || (reference_date.month() == birth_date.month()
                            && reference_date.day() < birth_date.day())
                    {
                        1
                    } else {
                        0
                    };

                    Some(years - adjustment)
                } else {
                    None // Not alive at the reference date
                }
            }
            None => None, // No birth date available
        }
    }
}
