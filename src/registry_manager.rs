//! Registry Manager for working with multiple data sources
//!
//! This module provides a high-level interface for working with multiple Danish
//! registry data sources, with optimized loading, caching, and filtering capabilities.

use crate::RecordBatch;
use crate::SchemaRef;
use crate::error::{Error, Result};
use crate::pnr_filter::{apply_filter_plan, build_filter_plan};
use crate::registry::{RegisterLoader, registry_from_name, registry_from_path};

use futures::future::join_all;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

/// Manager for working with multiple Danish registry data sources
///
/// This provides a high-level interface for loading, caching, and filtering
/// data from multiple registry sources.
pub struct RegistryManager {
    /// Registered loaders
    loaders: RwLock<HashMap<String, Arc<dyn RegisterLoader>>>,

    /// Registry paths
    paths: RwLock<HashMap<String, PathBuf>>,

    /// Cached data
    data_cache: RwLock<HashMap<String, Vec<RecordBatch>>>,

    /// Cached filtered data
    filtered_cache: Mutex<HashMap<String, HashMap<String, Vec<RecordBatch>>>>,

    /// Cache size limit
    max_cache_entries: usize,

    /// Join relationships between registries
    joins: HashMap<String, (String, String, String)>, // (child, parent, parent_column, child_column)
}

impl RegistryManager {
    /// Create a new registry manager
    #[must_use]
    pub fn new() -> Self {
        Self {
            loaders: RwLock::new(HashMap::new()),
            paths: RwLock::new(HashMap::new()),
            data_cache: RwLock::new(HashMap::new()),
            filtered_cache: Mutex::new(HashMap::new()),
            max_cache_entries: 20, // Default cache size
            joins: HashMap::new(),
        }
    }

    /// Create a new registry manager with specified joins
    #[must_use]
    pub fn with_joins(joins: HashMap<String, (String, String, String)>) -> Self {
        Self {
            loaders: RwLock::new(HashMap::new()),
            paths: RwLock::new(HashMap::new()),
            data_cache: RwLock::new(HashMap::new()),
            filtered_cache: Mutex::new(HashMap::new()),
            max_cache_entries: 20,
            joins,
        }
    }

    /// Register a data source
    pub fn register(&self, name: &str, path: &Path) -> Result<()> {
        // Create a registry loader
        let loader = registry_from_name(name)?;

        // Register the loader and path
        self.loaders.write()
            .map_err(|_| Error::InvalidOperation("Failed to acquire lock on loaders".to_string()))?
            .insert(name.to_string(), loader);

        self.paths
            .write()
            .map_err(|_| Error::InvalidOperation("Failed to acquire lock on paths".to_string()))?
            .insert(name.to_string(), path.to_path_buf());

        Ok(())
    }

    /// Register a data source with auto-detection
    pub fn register_auto(&self, path: &Path) -> Result<String> {
        // Try to infer registry type from path
        let loader = registry_from_path(path)?;
        let name = loader.get_register_name().to_string();

        // Register the loader and path
        self.loaders.write()
            .map_err(|_| Error::InvalidOperation("Failed to acquire lock on loaders".to_string()))?
            .insert(name.clone(), loader);

        self.paths
            .write()
            .map_err(|_| Error::InvalidOperation("Failed to acquire lock on paths".to_string()))?
            .insert(name.clone(), path.to_path_buf());

        Ok(name)
    }

    /// Register multiple data sources
    pub fn register_multiple(&self, sources: &[(&str, &Path)]) -> Result<()> {
        for (name, path) in sources {
            self.register(name, path)?;
        }

        Ok(())
    }

    /// Load data from a registered source
    pub fn load(&self, name: &str) -> Result<Vec<RecordBatch>> {
        // First check if data is already cached
        {
            let cache = self.data_cache.read().map_err(|_| {
                Error::InvalidOperation("Failed to acquire read lock on data cache".to_string())
            })?;

            if let Some(data) = cache.get(name) {
                return Ok(data.clone());
            }
        }

        // Get loader and path from the registry
        let loader = self.loaders.read()
            .map_err(|_| Error::InvalidOperation("Failed to acquire read lock on loaders".to_string()))?
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No loader registered for {name}")))?
            .clone();

        let path = self.paths.read()
            .map_err(|_| Error::InvalidOperation("Failed to acquire read lock on paths".to_string()))?
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No path registered for {name}")))?
            .clone();

        // Load the data
        let data = loader.load(&path, None)?;

        // Cache the data
        let mut cache = self.data_cache.write().map_err(|_| {
            Error::InvalidOperation("Failed to acquire write lock on data cache".to_string())
        })?;

        // Check if we need to evict from cache
        if cache.len() >= self.max_cache_entries {
            self.evict_cache(&mut cache);
        }

        cache.insert(name.to_string(), data.clone());
        drop(cache);

        Ok(data)
    }

    /// Load data from a registered source asynchronously
    ///
    /// # Errors
    /// Returns an error if the loader or path is not found, or if there's a problem loading the data
    pub async fn load_async(&self, name: &str) -> Result<Vec<RecordBatch>> {
        // First check if data is already cached
        {
            let cache = self.data_cache.read().map_err(|_| {
                Error::InvalidOperation("Failed to acquire read lock on data cache".to_string())
            })?;

            if let Some(data) = cache.get(name) {
                return Ok(data.clone());
            }
        }

        // Get loader and path from the registry
        let loader = self.loaders.read()
            .map_err(|_| Error::InvalidOperation("Failed to acquire read lock on loaders".to_string()))?
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No loader registered for {name}")))?
            .clone();

        let path = self.paths.read()
            .map_err(|_| Error::InvalidOperation("Failed to acquire read lock on paths".to_string()))?
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No path registered for {name}")))?
            .clone();

        // Now we can safely use await since we no longer hold any read locks

        // Load the data asynchronously
        let data = loader.load_async(&path, None).await?;

        // Cache the data
        let mut cache = self.data_cache.write().map_err(|_| {
            Error::InvalidOperation("Failed to acquire write lock on data cache".to_string())
        })?;

        // Check if we need to evict from cache
        if cache.len() >= self.max_cache_entries {
            self.evict_cache(&mut cache);
        }

        cache.insert(name.to_string(), data.clone());
        drop(cache);

        Ok(data)
    }

    /// Load data from multiple registered sources
    pub fn load_multiple(&self, names: &[&str]) -> Result<HashMap<String, Vec<RecordBatch>>> {
        // Use rayon for parallel loading
        let results: HashMap<String, Result<Vec<RecordBatch>>> = names
            .par_iter()
            .map(|&name| (name.to_string(), self.load(name)))
            .collect();

        // Process results
        let mut data = HashMap::with_capacity(names.len());
        for (name, result) in results {
            match result {
                Ok(batches) => {
                    data.insert(name, batches);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(data)
    }

    /// Load data from multiple registered sources asynchronously
    ///
    /// # Errors
    /// Returns an error if any registry loader or path is not found, or if there's a problem loading data
    pub async fn load_multiple_async(
        &self,
        names: &[&str],
    ) -> Result<HashMap<String, Vec<RecordBatch>>> {
        // Create futures for each load operation
        let futures = names
            .iter()
            .map(|&name| {
                let name_owned = name.to_string();
                async move {
                    let result = self.load_async(&name_owned).await;
                    (name_owned, result)
                }
            })
            .collect::<Vec<_>>();

        // Wait for all futures to complete
        let results = join_all(futures).await;

        // Process results
        let mut data = HashMap::with_capacity(names.len());
        for (name, result) in results {
            match result {
                Ok(batches) => {
                    data.insert(name, batches);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(data)
    }

    /// Filter data by PNR values
    pub fn filter_by_pnr(
        &self,
        names: &[&str],
        pnr_filter: &HashSet<String>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>> {
        // First, check if filtered data is already cached
        let cache_key = Self::generate_cache_key(pnr_filter);
        {
            let filtered_cache = self.filtered_cache.lock().map_err(|_| {
                Error::InvalidOperation("Failed to acquire lock on filtered cache".to_string())
            })?;

            if let Some(cache_entry) = filtered_cache.get(&cache_key) {
                let mut result = HashMap::new();
                for &name in names {
                    if let Some(data) = cache_entry.get(name) {
                        result.insert(name.to_string(), data.clone());
                    }
                }

                if result.len() == names.len() {
                    return Ok(result);
                }
            }
        }

        // Not cached, load and filter the data
        let data = self.load_multiple(names)?;

        // Build a filter plan
        let schemas = self.get_schemas(names)?;
        let joins = self.get_joins_for_registries(names);
        let pnr_columns = self.get_pnr_columns(names)?;

        let plan = build_filter_plan(&schemas, &joins, &pnr_columns);

        // Apply the filter plan
        let filtered_data = apply_filter_plan(&plan, &data, pnr_filter)?;

        // Cache the filtered data
        {
            let mut filtered_cache = self.filtered_cache.lock().map_err(|_| {
                Error::InvalidOperation("Failed to acquire lock on filtered cache".to_string())
            })?;

            // Add to cache
            filtered_cache
                .entry(cache_key)
                .or_insert_with(HashMap::new)
                .extend(filtered_data.clone());

            // Evict from cache if needed
            if filtered_cache.len() > self.max_cache_entries {
                // Remove oldest entry
                if let Some(oldest_key) = filtered_cache.keys().next().cloned() {
                    filtered_cache.remove(&oldest_key);
                }
            }
        }

        Ok(filtered_data)
    }

    /// Filter data by PNR values asynchronously
    ///
    /// # Errors
    /// Returns an error if the loaders or paths are not found, or if there's a problem loading or filtering the data
    pub async fn filter_by_pnr_async(
        &self,
        names: &[&str],
        pnr_filter: &HashSet<String>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>> {
        // First, check if filtered data is already cached
        let cache_key = Self::generate_cache_key(pnr_filter);
        let names_vec = names.iter().map(|&s| s.to_string()).collect::<Vec<_>>();
        let pnr_filter_cloned = pnr_filter.clone();

        // Check cache first
        {
            let filtered_cache = self.filtered_cache.lock().map_err(|_| {
                Error::InvalidOperation("Failed to acquire lock on filtered cache".to_string())
            })?;

            if let Some(cache_entry) = filtered_cache.get(&cache_key) {
                let mut result = HashMap::new();
                for name in &names_vec {
                    if let Some(data) = cache_entry.get(name) {
                        result.insert(name.clone(), data.clone());
                    }
                }

                if result.len() == names_vec.len() {
                    return Ok(result);
                }
            }
        }

        // Prepare the filter plan data before calling async operations
        let (schemas, joins, pnr_columns) = {
            let schemas = self.get_schemas(names)?;
            let joins = self.get_joins_for_registries(names);
            let pnr_columns = self.get_pnr_columns(names)?;

            (schemas, joins, pnr_columns)
        };

        // Build the filter plan before any async operations
        let plan = build_filter_plan(&schemas, &joins, &pnr_columns);

        // Not cached, load and filter the data asynchronously
        // Use string slices as the names to pass to load_multiple_async
        let name_refs: Vec<&str> = names_vec.iter().map(std::string::String::as_str).collect();
        let data = self.load_multiple_async(&name_refs).await?;

        // Apply the filter plan
        let filtered_data = apply_filter_plan(&plan, &data, &pnr_filter_cloned)?;

        // Cache the filtered data
        {
            let mut filtered_cache = self.filtered_cache.lock().map_err(|_| {
                Error::InvalidOperation("Failed to acquire lock on filtered cache".to_string())
            })?;

            // Add to cache
            filtered_cache
                .entry(cache_key)
                .or_insert_with(HashMap::new)
                .extend(filtered_data.clone());

            // Evict from cache if needed
            if filtered_cache.len() > self.max_cache_entries {
                // Remove oldest entry
                if let Some(oldest_key) = filtered_cache.keys().next().cloned() {
                    filtered_cache.remove(&oldest_key);
                }
            }
        }

        Ok(filtered_data)
    }

    /// Check if a registry is already registered
    pub fn has_registry(&self, name: &str) -> bool {
        match self.loaders.read() {
            Ok(loaders) => loaders.contains_key(name),
            Err(_) => false // In case of error, assume not registered
        }
    }

    /// Get the schema for a registry
    pub fn get_schema(&self, name: &str) -> Result<SchemaRef> {
        // Get the loader directly to avoid holding the lock longer than necessary
        let loader = self
            .loaders
            .read()
            .map_err(|_| {
                Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
            })?
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No loader registered for {name}")))?
            .clone();

        Ok(loader.get_schema())
    }

    /// Set the cache size limit
    pub const fn set_cache_size(&mut self, size: usize) {
        self.max_cache_entries = size;
    }

    /// Clear all caches
    pub fn clear_caches(&self) -> Result<()> {
        // Clear data cache - drop lock immediately after clearing
        self.data_cache
            .write()
            .map_err(|_| {
                Error::InvalidOperation("Failed to acquire write lock on data cache".to_string())
            })?
            .clear();

        // Clear filtered cache - drop lock immediately after clearing
        self.filtered_cache
            .lock()
            .map_err(|_| {
                Error::InvalidOperation("Failed to acquire lock on filtered cache".to_string())
            })?
            .clear();

        Ok(())
    }

    // Helper functions

    /// Generate a cache key for the given PNR filter
    fn generate_cache_key(pnr_filter: &HashSet<String>) -> String {
        // Sort the PNRs to ensure consistent cache keys
        let mut pnrs: Vec<&String> = pnr_filter.iter().collect();
        pnrs.sort();

        // Create a cache key with the first 5 PNRs and count
        let prefix: String = pnrs.iter().take(5).map(|s| s.as_str()).collect();
        format!("{}_{}", prefix, pnr_filter.len())
    }

    /// Evict entries from the cache
    fn evict_cache(&self, cache: &mut HashMap<String, Vec<RecordBatch>>) {
        // Simple eviction strategy: remove oldest entries (first 25%)
        let num_to_remove = self.max_cache_entries / 4;

        // Get keys to remove (can't just remove while iterating)
        let keys_to_remove: Vec<String> = cache.keys().take(num_to_remove).cloned().collect();

        // Remove the entries
        for key in keys_to_remove {
            cache.remove(&key);
        }
    }

    /// Get schemas for the specified registries
    fn get_schemas(&self, names: &[&str]) -> Result<HashMap<String, SchemaRef>> {
        // Get a clone of all loaders we need at once to minimize lock time
        let loaders_map = {
            let loaders = self.loaders.read().map_err(|_| {
                Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
            })?;

            let mut loader_map = HashMap::with_capacity(names.len());
            for &name in names {
                if let Some(loader) = loaders.get(name) {
                    loader_map.insert(name.to_string(), loader.clone());
                } else {
                    return Err(
                        Error::ValidationError(format!("No loader registered for {name}")).into(),
                    );
                }
            }
            loader_map
        };

        // Build the schemas map
        let mut schemas = HashMap::with_capacity(names.len());
        for (name, loader) in loaders_map {
            schemas.insert(name, loader.get_schema());
        }

        Ok(schemas)
    }

    /// Get PNR column names for the specified registries
    fn get_pnr_columns(&self, names: &[&str]) -> Result<HashMap<String, String>> {
        // Get a clone of all loaders we need at once to minimize lock time
        let loaders_map = {
            let loaders = self.loaders.read().map_err(|_| {
                Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
            })?;

            let mut loader_map = HashMap::with_capacity(names.len());
            for &name in names {
                if let Some(loader) = loaders.get(name) {
                    loader_map.insert(name.to_string(), loader.clone());
                } else {
                    return Err(
                        Error::ValidationError(format!("No loader registered for {name}")).into(),
                    );
                }
            }
            loader_map
        };

        // Build the pnr_columns map
        let mut pnr_columns = HashMap::with_capacity(names.len());
        for (name, loader) in loaders_map {
            if let Some(column) = loader.get_pnr_column_name() {
                pnr_columns.insert(name, column.to_string());
            }
        }

        Ok(pnr_columns)
    }

    /// Get join relationships for the specified registries
    fn get_joins_for_registries(&self, names: &[&str]) -> HashMap<String, (String, String)> {
        let mut joins = HashMap::new();

        for &name in names {
            if let Some((parent, parent_col, _child_col)) = self.joins.get(name) {
                if names.contains(&parent.as_str()) {
                    joins.insert(name.to_string(), (parent.clone(), parent_col.clone()));
                }
            }
        }

        joins
    }
}

impl Default for RegistryManager {
    fn default() -> Self {
        Self::new()
    }
}
