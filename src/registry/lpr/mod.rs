//! LPR (Landspatientregistret) registry loaders
//!
//! This module contains registry loaders for different versions of the Danish National Patient Registry (LPR).
//! Includes loaders for different versions and types of LPR data, along with
//! direct model conversion capabilities.
//!
//! The LPR data is split into two main versions:
//! - LPR version 2 (1977-2019): Contains adm (admissions), diag (diagnoses), and bes (outpatient visits)
//! - LPR version 3 (2019-present): Contains kontakter (contacts) and diagnoser (diagnoses)
//!
//! Each version has its own schema and data structure, but both provide health information
//! about patients in the Danish healthcare system.

// Re-export registry structs for easier access
pub use v2::adm::{create_deserializer as create_adm_deserializer, LprAdmRegistry};
pub use v2::diag::{create_deserializer as create_diag_deserializer, LprDiagRegistry};
pub use v2::bes::{create_deserializer as create_bes_deserializer, LprBesRegistry};

pub use v3::kontakter::{create_deserializer as create_kontakter_deserializer, Lpr3KontakterRegistry};
pub use v3::diagnoser::{create_deserializer as create_diagnoser_deserializer, Lpr3DiagnoserRegistry};

// Version-specific modules
pub mod v2;
pub mod v3;