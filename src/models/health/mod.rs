//! Health-related models module
//!
//! This module contains models related to health status, diagnoses,
//! and medical conditions.

pub mod diagnosis;
pub mod mapper;

pub use diagnosis::Diagnosis;
pub use mapper::{DiagnosisMapper, RecnumProvider, PnrProvider, RecnumToPnrMap};