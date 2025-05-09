//! Registry definitions and loaders for various Danish data sources
//!
//! This module contains registry definitions and loaders for various Danish registry data sources.
//! It provides a unified interface for loading and processing parquet files from different
//! registry sources, with optimized loading strategies for each.
//!
//! This module also contains the implementations for converting registry data to domain models,
//! supporting direct model conversion capabilities.
//!
//! Available registries:
//! - AKM (Arbejdsklassifikationsmodulet): Employment information
//! - BEF (Befolkning): Population demographic information
//! - DOD (Deaths): Death records
//! - DODSAARSAG (Causes of death): Death cause information
//! - (Removed IDAN registry)
//! - IND (Indkomst): Income and tax information
//! - LPR (Landspatientregistret): National Patient Registry (versions 2 and 3)
//! - MFR (Medical Birth Registry): Birth information
//! - UDDF (Uddannelse): Educational information
//! - VNDS (Vandringer/Migration): Migration information

use crate::RecordBatch;
use crate::Result;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::path::Path;

use std::future::Future;
use std::pin::Pin;

/// Base trait for registry loaders
pub trait RegisterLoader: Send + Sync {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str;

    /// Get the schema for this register
    fn get_schema(&self) -> SchemaRef;

    /// Load records from the register
    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>>;

    /// Load records from the register asynchronously
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>>;

    /// Returns whether this registry supports direct PNR filtering
    /// Some registries require joins to filter by PNR
    fn supports_pnr_filter(&self) -> bool {
        true
    }

    /// Returns the column name containing the PNR, if any
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("PNR")
    }

    /// Returns the join column name if this registry needs to be joined with another to get PNRs
    fn get_join_column_name(&self) -> Option<&'static str> {
        None
    }
}

// Will contain registry implementations
pub mod akm;
pub mod bef;
pub mod death {
    pub mod dod;
    pub mod dodsaarsag;
}
// Removed IDAN registry
pub mod ind;
pub mod lpr;
pub mod mfr;
pub mod uddf;
pub mod vnds;

// Re-export registry structs for easier access
pub use akm::AkmRegister;
pub use bef::BefRegister;
pub use bef::BefCombinedRegister;
pub use death::dod::DodRegister;
pub use death::dodsaarsag::DodsaarsagRegister;
// Removed IdanRegister
pub use ind::IndRegister;
pub use ind::conversion::YearConfiguredIndRegister;
pub use lpr::{
    v2::{LprAdmRegister, LprBesRegister, LprDiagRegister},
    v3::{Lpr3DiagnoserRegister, Lpr3KontakterRegister},
    discovery::{LprPaths, find_lpr_files},
};
pub use mfr::MfrRegister;
pub use mfr::conversion::MfrChildRegister;
pub use uddf::UddfRegister;
pub use vnds::VndsRegister;

pub mod factory;
pub use factory::{load_multiple_registries, registry_from_name, registry_from_path};

mod transform;
pub use transform::{
    add_postal_code_region, add_year_column, filter_by_date_range, filter_out_missing_values,
    map_categorical_values, scale_numeric_values, transform_records,
};

// Model conversion traits and implementation
pub mod model_conversion;
pub use model_conversion::{ModelConversion, ModelConvertingRegisterLoader};

// Registry-specific model conversion implementations
pub mod registry_aware_models;
