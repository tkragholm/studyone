//! A Rust library for parsing and reading Parquet files with schema validation,
//! filtering, and async functionality.
//!
//! This library provides optimized tools for working with Danish registry data in Parquet format,
//! including schema validation, filtering, and async loading capabilities.

pub mod algorithm;
pub mod async_io;
pub mod collections;
pub mod common;
pub mod config;
pub mod error;
#[doc(inline)]
pub use error::ResultExt;
pub mod filter;
pub mod loader;
pub mod models;
pub mod pnr_filter;
pub mod reader;
pub mod registry;
pub mod registry_manager;
pub mod schema;
pub mod utils;

pub mod filter_expression;

// Examples are now in the examples directory

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
pub use models::core::individual::Individual;
// These models are commented out in the source models/mod.rs file
// When they're needed, they should be properly re-exported there first
// pub use models::derived::Child;
// pub use models::derived::Family;
// pub use models::derived::Parent;
// pub use models::economic::Income;
// pub use models::health::Diagnosis;

// Model collections
pub use collections::{
    // These collections are not yet properly exported
    // DiagnosisCollection,
    // FamilyCollection,
    // Generic collections
    GenericCollection,
    // Specialized collections
    IndividualCollection,
    RelatedModelCollection,
    TemporalCollectionWithCache,
};

// Common traits
pub use common::traits::{
    AdapterFactory,
    AsyncDirectoryLoader,
    AsyncFilterableLoader,
    // Async loading traits
    AsyncLoader,
    AsyncParallelLoader,
    AsyncPnrFilterableLoader,
    BatchCollection,
    BatchProcessor,
    BefRegistry,
    CacheableCollection,
    DodRegistry,
    IndRegistry,
    LookupCollection,
    LprRegistry,
    MfrRegistry,
    // Collection traits
    ModelCollection,
    ModelLookup,
    // Adapter traits
    RegistryAdapter,
    // Registry conversion traits
    RegistryAware,
    RelatedCollection,
    StatefulAdapter,
    TemporalCollection,
};

// Filtering capabilities
pub use filter::{Expr, LiteralValue};
pub use filter::{filter_record_batch, read_parquet_with_filter};

// Utility functions
pub use utils::{DEFAULT_BATCH_SIZE, load_parquet_files_parallel, read_parquet};

// Async functionality
pub use async_io::{
    Loader,
    // Standard async loaders
    ParquetLoader,
    load_parquet_files_parallel_async,
    load_parquet_files_parallel_with_filter_async,
    read_parquet_async,
    read_parquet_with_filter_async,
};
pub use filter::async_filtering::read_parquet_with_pnr_filter_async;

// Re-export the procedural macros for registry definitions
pub use macros::RegistryTrait;

// Registry factory functions
pub use registry::factory::{load_multiple_registries, registry_from_name, registry_from_path};

// PNR filtering utilities
pub use pnr_filter::{
    FilterPlan, apply_filter_plan, build_filter_plan, filter_batch_by_pnr, join_and_filter_by_pnr,
};

// Registry manager
pub use registry_manager::RegistryManager;

// Algorithm modules - commented out because the module is commented out in algorithm/mod.rs
// pub use algorithm::population::{
//     FilterCriteria, Population, PopulationBuilder, PopulationConfig, PopulationFilter,
//     RegistryIntegration,
// };
