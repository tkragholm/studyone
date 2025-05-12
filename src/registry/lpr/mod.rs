//! LPR (Landspatientregistret) registry loaders
//!
//! This module contains registry loaders for different versions of the Danish National Patient Registry (LPR).
//! Includes loaders for different versions and types of LPR data, along with
//! direct model conversion capabilities.

// Import submodules
pub mod discovery;
pub mod individual;
pub mod trait_deserializer;
pub mod v2;
pub mod v3;

// Re-export specific loaders
pub use self::conversion::PnrLookupRegistry;
pub use self::discovery::{LprPaths, find_lpr_files};
pub use self::v2::{LprAdmRegister, LprBesRegister, LprDiagRegister};
pub use self::v3::{Lpr3DiagnoserRegister, Lpr3KontakterRegister};

// Implement PNR lookup for LPR registries
impl PnrLookupRegistry for LprDiagRegister {
    fn get_pnr_lookup(&self) -> Option<std::collections::HashMap<String, String>> {
        // Call the struct's method directly
        self.pnr_lookup.clone()
    }

    fn set_pnr_lookup(&mut self, lookup: std::collections::HashMap<String, String>) {
        // Call the struct's method directly
        self.pnr_lookup = Some(lookup);
    }
}

impl PnrLookupRegistry for Lpr3DiagnoserRegister {
    fn get_pnr_lookup(&self) -> Option<std::collections::HashMap<String, String>> {
        self.pnr_lookup.clone()
    }

    fn set_pnr_lookup(&mut self, lookup: std::collections::HashMap<String, String>) {
        self.pnr_lookup = Some(lookup);
    }
}

// Note: LprRegistry trait is now implemented for Diagnosis in models/registry.rs
