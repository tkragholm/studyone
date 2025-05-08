//! A Rust library for parsing and reading Parquet files with schema validation,
//! filtering, and async functionality.
//!
//! This library provides optimized tools for working with Danish registry data in Parquet format,
//! including schema validation, filtering, and async loading capabilities.

//pub mod algorithm;
pub mod adapters;
pub mod async_io;
pub mod collections;
pub mod common;
pub mod config;
pub mod error;
#[doc(inline)]
pub use error::ResultExt;
pub mod filter;
pub mod models;
pub mod pnr_filter;
pub mod reader;
pub mod registry;
pub mod registry_manager;
pub mod schema;
pub mod utils;

// Examples
pub mod examples;

// Re-export the most common types for easier use
// Core types
pub use config::ParquetReaderConfig;
pub use error::{Error, ParquetReaderError, ParquetResult, Result};
pub use reader::{ParquetReader, ParquetRowIterator};
pub use schema::{SchemaCompatibilityReport, SchemaIssue};

// Arrow types
pub use arrow::datatypes::Schema as ArrowSchema;
pub use arrow::datatypes::SchemaRef;
pub use arrow::record_batch::RecordBatch;

// Domain models
pub use models::{Child, Diagnosis, Family, Income, Individual, Parent};

// Model collections
pub use collections::{
    // Generic collections
    GenericCollection, TemporalCollectionWithCache, RelatedModelCollection,
    // Specialized collections
    IndividualCollection, DiagnosisCollection, FamilyCollection
};

// Registry-model conversion
pub use registry::model_conversion::{ModelConversion, ModelConvertingRegisterLoader};

// Unified adapters
pub use adapters::{
    // Adapter factory and utils
    AdapterFactoryImpl, create_lookup_with,
    // BEF adapters
    BefIndividualAdapter, BefFamilyAdapter, BefCombinedAdapter,
    // IND adapters
    IndIncomeAdapter,
    // LPR adapters
    LprDiagnosisAdapter,
    // MFR adapters
    MfrChildAdapter,
};

// Common traits
pub use common::traits::{
    // Registry conversion traits
    RegistryAware, BefRegistry, IndRegistry, LprRegistry, MfrRegistry, DodRegistry,
    // Adapter traits
    RegistryAdapter, StatefulAdapter, AdapterFactory, BatchProcessor, ModelLookup,
    // Collection traits
    ModelCollection, TemporalCollection, BatchCollection, LookupCollection, 
    RelatedCollection, CacheableCollection,
    // Async loading traits
    AsyncLoader, AsyncFilterableLoader, AsyncPnrFilterableLoader, 
    AsyncDirectoryLoader, AsyncParallelLoader
};

// Filtering capabilities
pub use filter::{Expr, LiteralValue};
pub use filter::{filter_record_batch, read_parquet_with_filter};

// Utility functions
pub use utils::{DEFAULT_BATCH_SIZE, load_parquet_files_parallel, read_parquet};

// Async functionality
pub use async_io::{
    load_parquet_files_parallel_async, load_parquet_files_parallel_with_filter_async,
    read_parquet_async, read_parquet_with_filter_async,
    // Standard async loaders
    ParquetLoader, PnrFilterableLoader
};
pub use filter::async_filtering::read_parquet_with_pnr_filter_async;

// Registry functionality
pub use registry::{
    // Registry loaders
    AkmRegister,
    BefRegister,
    DodRegister,
    DodsaarsagRegister,
    IdanRegister,
    IndRegister,
    Lpr3DiagnoserRegister,
    Lpr3KontakterRegister,
    // LPR registry loaders
    LprAdmRegister,
    LprBesRegister,
    LprDiagRegister,
    LprPaths,
    MfrRegister,
    RegisterLoader,
    UddfRegister,
    VndsRegister,
    add_postal_code_region,
    add_year_column,
    filter_by_date_range,
    filter_out_missing_values,
    find_lpr_files,
    load_multiple_registries,
    map_categorical_values,
    // Registry factories
    registry_from_name,
    registry_from_path,
    scale_numeric_values,
    // Transformation utilities
    transform_records,
};

// PNR filtering utilities
pub use pnr_filter::{
    FilterPlan, apply_filter_plan, build_filter_plan, filter_batch_by_pnr, join_and_filter_by_pnr,
};

// Registry manager
pub use registry_manager::RegistryManager;

// Algorithm modules
// pub use algorithm::population::{
//     FilterCriteria, Population, PopulationBuilder, PopulationConfig, PopulationFilter,
//     RegistryIntegration,
// };
