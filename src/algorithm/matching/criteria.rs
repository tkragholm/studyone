//! Matching criteria definitions for case-control matching
//!
//! This module provides the structure and logic for defining matching criteria
//! used in the case-control matching algorithm.

# ![allow(unused_imports)]
use chrono::Duration;

/// Criteria for matching cases to controls
///
/// This struct defines the parameters used to determine whether a control
/// is an eligible match for a case. It includes parameters for various
/// demographic and socioeconomic factors.
#[derive(Debug, Clone)]
pub struct MatchingCriteria {
    /// Maximum allowed difference in days between birth dates of case and control
    pub birth_date_window_days: i32,

    /// Maximum allowed difference in days between parent birth dates of case and control
    pub parent_birth_date_window_days: i32,

    /// Whether both parents are required to be present for matching
    pub require_both_parents: bool,

    /// Whether the same gender is required for matching
    pub require_same_gender: bool,

    /// Whether to match on family size (number of siblings)
    pub match_family_size: bool,

    /// Maximum allowed difference in family size (number of siblings)
    pub family_size_tolerance: i32,

    /// Whether to match on parental education level
    pub match_education_level: bool,

    /// Whether to match on geographic location (municipality)
    pub match_geography: bool,
    
    /// Whether to match on parental relationship status
    pub match_parental_status: bool,
    
    /// Whether to match on immigrant background
    pub match_immigrant_background: bool,
}

impl Default for MatchingCriteria {
    fn default() -> Self {
        Self {
            birth_date_window_days: 30,             // Match within 1 month of birth
            parent_birth_date_window_days: 365,     // Match parent ages within 1 year
            require_both_parents: false,            // Allow single-parent families
            require_same_gender: true,              // Match on gender
            match_family_size: true,                // Match on family size
            family_size_tolerance: 1,               // Allow ±1 sibling difference
            match_education_level: false,           // Don't require matching education
            match_geography: false,                 // Don't require matching geography
            match_parental_status: false,           // Don't require matching parental status
            match_immigrant_background: false,      // Don't require matching immigrant background
        }
    }
}

impl MatchingCriteria {
    /// Create a new instance with default values
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new builder for constructing matching criteria
    #[must_use]
    pub fn builder() -> MatchingCriteriaBuilder {
        MatchingCriteriaBuilder::new()
    }

    /// Check if a birth date falls within the allowable window
    ///
    /// # Arguments
    ///
    /// * `case_birth_date` - The birth date of the case
    /// * `control_birth_date` - The birth date of the control
    ///
    /// # Returns
    ///
    /// `true` if the birth dates are within the allowable window, `false` otherwise
    #[must_use]
    pub fn is_birth_date_match(&self, case_birth_date: chrono::NaiveDate, control_birth_date: chrono::NaiveDate) -> bool {
        let diff = (control_birth_date - case_birth_date).num_days().abs() as i32;
        diff <= self.birth_date_window_days
    }

    /// Convert to a human-readable string representation
    #[must_use]
    pub fn to_string_representation(&self) -> String {
        format!(
            "Matching Criteria:\n\
             - Birth date window: ±{} days\n\
             - Parent birth date window: ±{} days\n\
             - Require both parents: {}\n\
             - Require same gender: {}\n\
             - Match family size: {}\n\
             - Family size tolerance: ±{}\n\
             - Match education level: {}\n\
             - Match geography: {}\n\
             - Match parental status: {}\n\
             - Match immigrant background: {}",
            self.birth_date_window_days,
            self.parent_birth_date_window_days,
            self.require_both_parents,
            self.require_same_gender,
            self.match_family_size,
            self.family_size_tolerance,
            self.match_education_level,
            self.match_geography,
            self.match_parental_status,
            self.match_immigrant_background
        )
    }
}

/// Builder for constructing matching criteria
#[derive(Debug, Clone)]
pub struct MatchingCriteriaBuilder {
    criteria: MatchingCriteria,
}

impl Default for MatchingCriteriaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MatchingCriteriaBuilder {
    /// Create a new builder with default criteria
    #[must_use]
    pub fn new() -> Self {
        Self {
            criteria: MatchingCriteria::default(),
        }
    }

    /// Set the birth date window in days
    #[must_use]
    pub const fn birth_date_window_days(mut self, days: i32) -> Self {
        self.criteria.birth_date_window_days = days;
        self
    }

    /// Set the parent birth date window in days
    #[must_use]
    pub const fn parent_birth_date_window_days(mut self, days: i32) -> Self {
        self.criteria.parent_birth_date_window_days = days;
        self
    }

    /// Set whether both parents are required
    #[must_use]
    pub const fn require_both_parents(mut self, required: bool) -> Self {
        self.criteria.require_both_parents = required;
        self
    }

    /// Set whether the same gender is required
    #[must_use]
    pub const fn require_same_gender(mut self, required: bool) -> Self {
        self.criteria.require_same_gender = required;
        self
    }

    /// Set whether to match on family size
    #[must_use]
    pub const fn match_family_size(mut self, match_size: bool) -> Self {
        self.criteria.match_family_size = match_size;
        self
    }

    /// Set the family size tolerance
    #[must_use]
    pub const fn family_size_tolerance(mut self, tolerance: i32) -> Self {
        self.criteria.family_size_tolerance = tolerance;
        self
    }

    /// Set whether to match on parental education level
    #[must_use]
    pub const fn match_education_level(mut self, match_education: bool) -> Self {
        self.criteria.match_education_level = match_education;
        self
    }

    /// Set whether to match on geographic location
    #[must_use]
    pub const fn match_geography(mut self, match_geography: bool) -> Self {
        self.criteria.match_geography = match_geography;
        self
    }

    /// Set whether to match on parental relationship status
    #[must_use]
    pub const fn match_parental_status(mut self, match_status: bool) -> Self {
        self.criteria.match_parental_status = match_status;
        self
    }

    /// Set whether to match on immigrant background
    #[must_use]
    pub const fn match_immigrant_background(mut self, match_background: bool) -> Self {
        self.criteria.match_immigrant_background = match_background;
        self
    }

    /// Build the matching criteria
    #[must_use]
    pub const fn build(self) -> MatchingCriteria {
        self.criteria
    }
}

/// Configuration for the matching process
#[derive(Debug, Clone)]
pub struct MatchingConfig {
    /// The criteria to use for matching
    pub criteria: MatchingCriteria,
    
    /// The ratio of controls to match to each case (e.g., 1:4 would be 4)
    pub matching_ratio: usize,
    
    /// Whether to use parallel processing for matching
    pub use_parallel: bool,
    
    /// Optional random seed for reproducible matching
    pub random_seed: Option<u64>,
    
    /// The date to use as the matching date (for fixed-time point approach)
    pub matching_date: Option<chrono::NaiveDate>,
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            criteria: MatchingCriteria::default(),
            matching_ratio: 1,
            use_parallel: true,
            random_seed: None,
            matching_date: None,
        }
    }
}

impl MatchingConfig {
    /// Create a new configuration with default values
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new builder for constructing matching configuration
    #[must_use]
    pub fn builder() -> MatchingConfigBuilder {
        MatchingConfigBuilder::new()
    }
}

/// Builder for constructing matching configuration
#[derive(Debug, Clone)]
pub struct MatchingConfigBuilder {
    config: MatchingConfig,
}

impl Default for MatchingConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MatchingConfigBuilder {
    /// Create a new builder with default configuration
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: MatchingConfig::default(),
        }
    }

    /// Set the matching criteria
    #[must_use]
    pub const fn criteria(mut self, criteria: MatchingCriteria) -> Self {
        self.config.criteria = criteria;
        self
    }

    /// Set the matching ratio
    #[must_use]
    pub const fn matching_ratio(mut self, ratio: usize) -> Self {
        self.config.matching_ratio = ratio;
        self
    }

    /// Set whether to use parallel processing
    #[must_use]
    pub const fn use_parallel(mut self, parallel: bool) -> Self {
        self.config.use_parallel = parallel;
        self
    }

    /// Set the random seed
    #[must_use]
    pub const fn random_seed(mut self, seed: u64) -> Self {
        self.config.random_seed = Some(seed);
        self
    }

    /// Set the matching date
    #[must_use]
    pub const fn matching_date(mut self, date: chrono::NaiveDate) -> Self {
        self.config.matching_date = Some(date);
        self
    }

    /// Build the matching configuration
    #[must_use]
    pub const fn build(self) -> MatchingConfig {
        self.config
    }
}