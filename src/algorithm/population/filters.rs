//! Population filtering criteria
//!
//! This module provides filters for narrowing down study populations
//! based on various demographic and clinical criteria.

use chrono::NaiveDate;
use std::collections::HashSet;

use crate::models::family::FamilySnapshot;
use crate::models::{Family, Individual};

/// Defines a criterion for filtering individuals or families
pub trait FilterCriteria<T> {
    /// Determine if an entity meets the filter criteria
    fn meets_criteria(&self, entity: &T) -> bool;
}

/// A filter that can be applied to an individual
pub enum IndividualFilter {
    /// Filter by age range
    AgeRange {
        /// Minimum age (inclusive)
        min_age: Option<u32>,
        /// Maximum age (inclusive)
        max_age: Option<u32>,
        /// Reference date for age calculation
        reference_date: NaiveDate,
    },
    /// Filter by residency status at a specific date
    ResidentAt(NaiveDate),
    /// Filter by gender
    Gender(crate::models::individual::Gender),
    /// Filter by geographic origin
    Origin(crate::models::individual::Origin),
    /// Filter by education level
    EducationLevel(crate::models::individual::EducationLevel),
    /// Filter by municipality code (exact match)
    Municipality(String),
    /// Filter by rural/urban status
    RuralArea(bool),
    /// Filter by specific PNR set
    PnrList(HashSet<String>),
    /// Combined filter that requires all criteria to be met
    All(Vec<IndividualFilter>),
    /// Combined filter that requires any criterion to be met
    Any(Vec<IndividualFilter>),
}

impl FilterCriteria<Individual> for IndividualFilter {
    fn meets_criteria(&self, individual: &Individual) -> bool {
        match self {
            Self::AgeRange {
                min_age,
                max_age,
                reference_date,
            } => {
                if let Some(age) = individual.age_at(reference_date) {
                    let age_u32 = age as u32;

                    // Check minimum age constraint if specified
                    if let Some(min) = min_age {
                        if age_u32 < *min {
                            return false;
                        }
                    }

                    // Check maximum age constraint if specified
                    if let Some(max) = max_age {
                        if age_u32 > *max {
                            return false;
                        }
                    }

                    true
                } else {
                    // Individual has no calculable age (missing birth date)
                    false
                }
            }
            Self::ResidentAt(date) => individual.was_resident_at(date),
            Self::Gender(gender) => individual.gender == *gender,
            Self::Origin(origin) => individual.origin == *origin,
            Self::EducationLevel(level) => individual.education_level == *level,
            Self::Municipality(code) => individual.municipality_code.as_deref() == Some(code),
            Self::RuralArea(is_rural) => individual.is_rural == *is_rural,
            Self::PnrList(pnrs) => pnrs.contains(&individual.pnr),
            Self::All(filters) => filters.iter().all(|f| f.meets_criteria(individual)),
            Self::Any(filters) => filters.iter().any(|f| f.meets_criteria(individual)),
        }
    }
}

/// A filter that can be applied to a family
pub enum FamilyFilter {
    /// Filter by family type
    FamilyType(crate::models::family::FamilyType),
    /// Filter by family size (number of children)
    FamilySize {
        /// Minimum number of children (inclusive)
        min_children: Option<usize>,
        /// Maximum number of children (inclusive)
        max_children: Option<usize>,
    },
    /// Filter by municipality code (exact match)
    Municipality(String),
    /// Filter by rural/urban status
    RuralArea(bool),
    /// Filter by presence of child with SCD
    HasChildWithSCD(bool),
    /// Filter by presence of parental comorbidity
    HasParentalComorbidity(bool),
    /// Filter by presence of family support network
    HasSupportNetwork(bool),
    /// Filter by validity at a specific date
    ValidAt(NaiveDate),
    /// Combined filter that requires all criteria to be met
    All(Vec<FamilyFilter>),
    /// Combined filter that requires any criterion to be met
    Any(Vec<FamilyFilter>),
}

impl FilterCriteria<Family> for FamilyFilter {
    fn meets_criteria(&self, family: &Family) -> bool {
        match self {
            Self::FamilyType(family_type) => family.family_type == *family_type,
            Self::FamilySize {
                min_children,
                max_children,
            } => {
                let size = family.family_size();

                // Check minimum size constraint if specified
                if let Some(min) = min_children {
                    if size < *min {
                        return false;
                    }
                }

                // Check maximum size constraint if specified
                if let Some(max) = max_children {
                    if size > *max {
                        return false;
                    }
                }

                true
            }
            Self::Municipality(code) => family.municipality_code.as_deref() == Some(code),
            Self::RuralArea(is_rural) => family.is_rural == *is_rural,
            Self::HasChildWithSCD(has_scd) => family.has_child_with_scd() == *has_scd,
            Self::HasParentalComorbidity(has_comorbidity) => {
                family.has_parental_comorbidity == *has_comorbidity
            }
            Self::HasSupportNetwork(has_network) => family.has_support_network == *has_network,
            Self::ValidAt(date) => family.was_valid_at(date),
            Self::All(filters) => filters.iter().all(|f| f.meets_criteria(family)),
            Self::Any(filters) => filters.iter().any(|f| f.meets_criteria(family)),
        }
    }
}

/// A filter that can be applied to a family snapshot at a specific point in time
pub enum FamilySnapshotFilter {
    /// Filter by family type
    FamilyType(crate::models::family::FamilyType),
    /// Filter by family size (number of children)
    FamilySize {
        /// Minimum number of children (inclusive)
        min_children: Option<usize>,
        /// Maximum number of children (inclusive)
        max_children: Option<usize>,
    },
    /// Filter by municipality code (exact match)
    Municipality(String),
    /// Filter by rural/urban status
    RuralArea(bool),
    /// Filter by presence of child with SCD
    HasChildWithSCD(bool),
    /// Filter by presence of parental comorbidity
    HasParentalComorbidity(bool),
    /// Filter by presence of family support network
    HasSupportNetwork(bool),
    /// Combined filter that requires all criteria to be met
    All(Vec<FamilySnapshotFilter>),
    /// Combined filter that requires any criterion to be met
    Any(Vec<FamilySnapshotFilter>),
}

impl FilterCriteria<FamilySnapshot> for FamilySnapshotFilter {
    fn meets_criteria(&self, snapshot: &FamilySnapshot) -> bool {
        match self {
            Self::FamilyType(family_type) => snapshot.family_type == *family_type,
            Self::FamilySize {
                min_children,
                max_children,
            } => {
                let size = snapshot.family_size();

                // Check minimum size constraint if specified
                if let Some(min) = min_children {
                    if size < *min {
                        return false;
                    }
                }

                // Check maximum size constraint if specified
                if let Some(max) = max_children {
                    if size > *max {
                        return false;
                    }
                }

                true
            }
            Self::Municipality(code) => snapshot.municipality_code.as_deref() == Some(code),
            Self::RuralArea(is_rural) => snapshot.is_rural == *is_rural,
            Self::HasChildWithSCD(has_scd) => snapshot.has_child_with_scd() == *has_scd,
            Self::HasParentalComorbidity(has_comorbidity) => {
                snapshot.has_parental_comorbidity == *has_comorbidity
            }
            Self::HasSupportNetwork(has_network) => snapshot.has_support_network == *has_network,
            Self::All(filters) => filters.iter().all(|f| f.meets_criteria(snapshot)),
            Self::Any(filters) => filters.iter().any(|f| f.meets_criteria(snapshot)),
        }
    }
}

/// Common filter functions for population filtering
pub struct PopulationFilter;

impl PopulationFilter {
    /// Filter a vector of individuals using the specified criteria
    pub fn filter_individuals<F>(individuals: Vec<Individual>, filter: F) -> Vec<Individual>
    where
        F: FilterCriteria<Individual>,
    {
        individuals
            .into_iter()
            .filter(|i| filter.meets_criteria(i))
            .collect()
    }

    /// Filter a vector of families using the specified criteria
    pub fn filter_families<F>(families: Vec<Family>, filter: F) -> Vec<Family>
    where
        F: FilterCriteria<Family>,
    {
        families
            .into_iter()
            .filter(|f| filter.meets_criteria(f))
            .collect()
    }

    /// Filter a vector of family snapshots using the specified criteria
    pub fn filter_snapshots<F>(snapshots: Vec<FamilySnapshot>, filter: F) -> Vec<FamilySnapshot>
    where
        F: FilterCriteria<FamilySnapshot>,
    {
        snapshots
            .into_iter()
            .filter(|s| filter.meets_criteria(s))
            .collect()
    }

    /// Create a case-control filter pair for selecting case and control groups
    /// with matching demographic characteristics
    #[must_use] pub fn create_case_control_filters() -> (FamilySnapshotFilter, FamilySnapshotFilter) {
        // Case filter: Families with SCD children
        let case_filter = FamilySnapshotFilter::HasChildWithSCD(true);

        // Control filter: Families without SCD children, but otherwise similar
        let control_filter = FamilySnapshotFilter::All(vec![
            FamilySnapshotFilter::HasChildWithSCD(false),
            // Add other matching criteria here (e.g., family size, type, etc.)
        ]);

        (case_filter, control_filter)
    }
}
