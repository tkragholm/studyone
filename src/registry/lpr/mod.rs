//! LPR (Landspatientregistret) registry loaders
//!
//! This module contains registry loaders for different versions of the Danish National Patient Registry (LPR).
//! Includes loaders for different versions and types of LPR data, along with
//! direct model conversion capabilities.

// Import submodules
pub mod discovery;
pub mod lpr2;
pub mod lpr3;
pub mod model_conversion;

// Re-export specific loaders
pub use self::discovery::{LprPaths, find_lpr_files};
pub use self::lpr2::{LprAdmRegister, LprBesRegister, LprDiagRegister};
pub use self::lpr3::{Lpr3DiagnoserRegister, Lpr3KontakterRegister};
pub use self::model_conversion::PnrLookupRegistry;

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
