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

pub mod criteria;
pub mod algorithm;
pub mod balance;

// Re-export key types
pub use criteria::MatchingCriteria;
pub use algorithm::{Matcher, MatchedPair, MatchingResult};
pub use balance::{BalanceMetric, BalanceReport, BalanceSummary};