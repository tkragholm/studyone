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
    #[must_use] pub fn new() -> Self {
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
    #[must_use] pub fn with_joins(joins: HashMap<String, (String, String, String)>) -> Self {
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
        let mut loaders = self.loaders.write().map_err(|_| {
            Error::InvalidOperation("Failed to acquire lock on loaders".to_string())
        })?;

        let mut paths = self
            .paths
            .write()
            .map_err(|_| Error::InvalidOperation("Failed to acquire lock on paths".to_string()))?;

        loaders.insert(name.to_string(), loader);
        paths.insert(name.to_string(), path.to_path_buf());

        Ok(())
    }

    /// Register a data source with auto-detection
    pub fn register_auto(&self, path: &Path) -> Result<String> {
        // Try to infer registry type from path
        let loader = registry_from_path(path)?;
        let name = loader.get_register_name().to_string();

        // Register the loader and path
        let mut loaders = self.loaders.write().map_err(|_| {
            Error::InvalidOperation("Failed to acquire lock on loaders".to_string())
        })?;

        let mut paths = self
            .paths
            .write()
            .map_err(|_| Error::InvalidOperation("Failed to acquire lock on paths".to_string()))?;

        loaders.insert(name.clone(), loader);
        paths.insert(name.clone(), path.to_path_buf());

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

        // Data not cached, load it
        let loaders = self.loaders.read().map_err(|_| {
            Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
        })?;

        let paths = self.paths.read().map_err(|_| {
            Error::InvalidOperation("Failed to acquire read lock on paths".to_string())
        })?;

        let loader = loaders
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No loader registered for {name}")))?;

        let path = paths
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No path registered for {name}")))?;

        // Load the data
        let data = loader.load(path, None)?;

        // Cache the data
        let mut cache = self.data_cache.write().map_err(|_| {
            Error::InvalidOperation("Failed to acquire write lock on data cache".to_string())
        })?;

        // Check if we need to evict from cache
        if cache.len() >= self.max_cache_entries {
            self.evict_cache(&mut cache);
        }

        cache.insert(name.to_string(), data.clone());

        Ok(data)
    }

    /// Load data from a registered source asynchronously
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

        // Data not cached, load it
        let loaders = self.loaders.read().map_err(|_| {
            Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
        })?;

        let paths = self.paths.read().map_err(|_| {
            Error::InvalidOperation("Failed to acquire read lock on paths".to_string())
        })?;

        let loader = loaders
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No loader registered for {name}")))?;

        let path = paths
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No path registered for {name}")))?;

        // Load the data asynchronously
        let data = loader.load_async(path, None).await?;

        // Cache the data
        let mut cache = self.data_cache.write().map_err(|_| {
            Error::InvalidOperation("Failed to acquire write lock on data cache".to_string())
        })?;

        // Check if we need to evict from cache
        if cache.len() >= self.max_cache_entries {
            self.evict_cache(&mut cache);
        }

        cache.insert(name.to_string(), data.clone());

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
                    let result = self.load_async(name).await;
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
        let cache_key = self.generate_cache_key(pnr_filter);
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
    pub async fn filter_by_pnr_async(
        &self,
        names: &[&str],
        pnr_filter: &HashSet<String>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>> {
        // First, check if filtered data is already cached
        let cache_key = self.generate_cache_key(pnr_filter);
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

        // Not cached, load and filter the data asynchronously
        let data = self.load_multiple_async(names).await?;

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

    /// Get the schema for a registry
    pub fn get_schema(&self, name: &str) -> Result<SchemaRef> {
        let loaders = self.loaders.read().map_err(|_| {
            Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
        })?;

        let loader = loaders
            .get(name)
            .ok_or_else(|| Error::ValidationError(format!("No loader registered for {name}")))?;

        Ok(loader.get_schema())
    }

    /// Set the cache size limit
    pub const fn set_cache_size(&mut self, size: usize) {
        self.max_cache_entries = size;
    }

    /// Clear all caches
    pub fn clear_caches(&self) -> Result<()> {
        // Clear data cache
        let mut data_cache = self.data_cache.write().map_err(|_| {
            Error::InvalidOperation("Failed to acquire write lock on data cache".to_string())
        })?;

        data_cache.clear();

        // Clear filtered cache
        let mut filtered_cache = self.filtered_cache.lock().map_err(|_| {
            Error::InvalidOperation("Failed to acquire lock on filtered cache".to_string())
        })?;

        filtered_cache.clear();

        Ok(())
    }

    // Helper functions

    /// Generate a cache key for the given PNR filter
    fn generate_cache_key(&self, pnr_filter: &HashSet<String>) -> String {
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
        let loaders = self.loaders.read().map_err(|_| {
            Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
        })?;

        let mut schemas = HashMap::with_capacity(names.len());

        for &name in names {
            let loader = loaders.get(name).ok_or_else(|| {
                Error::ValidationError(format!("No loader registered for {name}"))
            })?;

            schemas.insert(name.to_string(), loader.get_schema());
        }

        Ok(schemas)
    }

    /// Get PNR column names for the specified registries
    fn get_pnr_columns(&self, names: &[&str]) -> Result<HashMap<String, String>> {
        let loaders = self.loaders.read().map_err(|_| {
            Error::InvalidOperation("Failed to acquire read lock on loaders".to_string())
        })?;

        let mut pnr_columns = HashMap::with_capacity(names.len());

        for &name in names {
            let loader = loaders.get(name).ok_or_else(|| {
                Error::ValidationError(format!("No loader registered for {name}"))
            })?;

            if let Some(column) = loader.get_pnr_column_name() {
                pnr_columns.insert(name.to_string(), column.to_string());
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
                    joins.insert(
                        name.to_string(),
                        (parent.clone(), parent_col.clone()),
                    );
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
