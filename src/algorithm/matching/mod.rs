//! Case-Control Matching algorithms for the research study workflow
//!
//! This module implements the algorithms for matching cases to controls
//! as described in the study flow. It includes:
//!
//! 1. Matching criteria definition
//! 2. Core matching algorithm with optimized parallel processing
//! 3. Covariate balance assessment for matched case-control pairs
//!
//! The implementation supports different matching ratios (1:1, 1:N) and
//! allows for customizable matching criteria based on various attributes
//! like age, gender, and socioeconomic factors.

// Module declarations
pub mod balance;
pub mod case_group;
pub mod control_data;
pub mod criteria;
pub mod extraction;
pub mod filtering;
pub mod matcher;
pub mod parallel;
pub mod sequential;
pub mod types;
pub mod validation;

// Legacy module - will be deprecated once refactoring is complete
// Kept for backward compatibility
//pub mod algorithm;

// Re-export key types for convenience
pub use balance::{BalanceMetric, BalanceReport, BalanceSummary};
pub use criteria::MatchingConfig;
pub use criteria::MatchingCriteria;
pub use matcher::Matcher;
pub use types::MatchedPair;
pub use types::MatchingResult;
