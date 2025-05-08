# Direct Registry-Model Integration Design

This document outlines the new approach for direct integration between registry data and domain models in the par-reader codebase, eliminating the need for separate adapter modules.

## Background

Previously, the application used a three-layer approach:
1. Registry modules: Handle loading raw data from parquet files
2. Schema modules: Define data structures for registry files
3. Adapter modules: Convert registry data to domain models

This separation created manual translation overhead, with adapters having to handle field mappings, type conversions, and business logic repeatedly. The new design eliminates this separation completely by allowing registries to directly convert to models and vice versa.

## Direct Integration Design

The new design provides direct integration between registry modules and domain models through trait implementations. This approach eliminates the need for separate adapter modules, reduces duplication, makes the code more maintainable, and provides a more intuitive API.

### Key Components

1. **ModelConversion Trait**
   ```rust
   pub trait ModelConversion<T> {
       fn to_models(&self, batch: &RecordBatch) -> Result<Vec<T>>;
       fn from_models(&self, models: &[T]) -> Result<RecordBatch>;
       fn transform_models(&self, models: &mut [T]) -> Result<()>;
   }
   ```

2. **ModelConversionExt Trait**
   Provides convenience methods for direct loading of domain models from registries:
   ```rust
   pub trait ModelConversionExt {
       fn load_as<T>(&self, base_path: &Path, pnr_filter: Option<&HashSet<String>>) -> Result<Vec<T>>
       where Self: ModelConversion<T> + RegisterLoader;
   
       fn load_as_async<'a, T>(...) -> Result<Vec<T>>
       where Self: ModelConversion<T> + RegisterLoader;
   }
   ```

3. **Schema-Aware Model Constructors**
   Models now include constructors that understand specific registry schemas:
   ```rust
   impl Individual {
       pub fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Self> { ... }
       pub fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> { ... }
   }
   ```

4. **Registry Model Conversion Implementations**
   Registries implement conversion to specific model types:
   ```rust
   impl ModelConversion<Individual> for BefRegister { ... }
   impl ModelConversion<Family> for BefRegister { ... }
   ```

## Usage Examples

### Loading Individuals from BEF Registry

```rust
// Create a BEF registry loader
let bef_registry = BefRegister::new();

// Load BEF data directly as Individual models
let individuals = bef_registry.load_as::<Individual>(base_path, None)?;

// With PNR filter
let pnr_filter = HashSet::from(["1234567890".to_string()]);
let filtered_individuals = bef_registry.load_as::<Individual>(base_path, Some(&pnr_filter))?;

// Async loading
let individuals = bef_registry.load_as_async::<Individual>(base_path, None).await?;
```

### Loading Families from BEF Registry

```rust
// Create a BEF registry loader
let bef_registry = BefRegister::new();

// Load BEF data directly as Family models
let families = bef_registry.load_as::<Family>(base_path, None)?;
```

## Benefits

1. **Reduced Code Duplication**
   - Registry-specific logic is centralized in the registry modules
   - Common conversion patterns are implemented once

2. **Type Safety**
   - The trait system ensures correct model types are used with appropriate registries
   - Schema compatibility is enforced at compile time

3. **Improved Maintainability**
   - Changes to registry schemas only need updates in one place
   - Adding new model types is easier with the trait system

4. **Better API**
   - More intuitive API for loading models directly
   - Consistent pattern across all registry types

5. **Leverages Schema Adaptation**
   - Uses the existing schema adaptation system for type conversions
   - Robust error handling for type mismatches

## Future Improvements

1. **Complete Bidirectional Conversion**
   - Implement the `from_models` method for all registry-model pairs
   - Enable round-trip conversions for all data types

2. **Additional Registry Support**
   - Extend the pattern to all registry types
   - Create standardized constructors for all model types

3. **Optimize Batch Processing**
   - Add batch processing optimizations for large datasets
   - Implement parallel model construction for performance

4. **Schema Validation**
   - Add runtime schema validation
   - Provide helpful error messages for schema mismatches