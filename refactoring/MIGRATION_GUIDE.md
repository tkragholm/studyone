# Migration Guide for Par-Reader Refactoring

This guide outlines how to migrate existing code to the new trait-based architecture, providing a clear path for removing legacy code and adopting the new patterns.

## Overview

As part of our refactoring efforts, we've introduced new trait-based patterns for:

1. **Registry Loading** - Using the AsyncLoader traits for standardized data loading
2. **Model Structure** - Separating entity models from registry-specific conversion
3. **Filtering** - Providing consistent filtering patterns across registries

This guide will help you migrate your code to the new patterns.

## Migrating Registry Implementations

### Old Pattern

```rust
// Old pattern with direct function calls
impl RegisterLoader for BefRegister {
    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
        let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);
        load_parquet_files_parallel(
            base_path,
            Some(self.schema.as_ref()),
            pnr_filter_ref,
            None,
            None,
        )
    }
    
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
            let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);
            load_parquet_files_parallel_with_pnr_filter_async(
                base_path,
                Some(self.schema.as_ref()),
                pnr_filter_ref,
            )
            .await
        })
    }
}
```

### New Pattern

```rust
// New pattern with trait-based loader
impl RegisterLoader for BefRegister {
    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        let rt = tokio::runtime::Runtime::new()?;
        
        rt.block_on(async {
            if let Some(filter) = pnr_filter {
                self.loader.load_with_pnr_filter_async(base_path, Some(filter)).await
            } else {
                self.loader.load_directory_async(base_path).await
            }
        })
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        if let Some(filter) = pnr_filter {
            self.loader.load_with_pnr_filter_async(base_path, Some(filter))
        } else {
            self.loader.load_directory_async(base_path)
        }
    }
}
```

### Migration Steps

1. **Create a Loader Instance**: 
   ```rust
   let loader = PnrFilterableLoader::with_schema_ref(schema.clone())
       .with_pnr_column("PNR");
   ```

2. **Replace Direct Function Calls**:
   - Replace calls to `load_parquet_files_parallel` with `loader.load_directory_async`
   - Replace calls to `load_parquet_files_parallel_with_pnr_filter_async` with `loader.load_with_pnr_filter_async`

3. **Handle PNR Filtering**:
   - Use the `loader.load_with_pnr_filter_async()` method instead of custom PNR filtering

4. **Add Blocking Runtime**:
   - For sync methods, wrap the async code in a runtime using `tokio::runtime::Runtime`

## Migrating Model Conversion

### Old Pattern

```rust
// Old pattern with direct model construction in registry files
impl Individual {
    pub fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Direct model construction logic with registry-specific columns
    }
}
```

### New Pattern

```rust
// New pattern with separated concerns
// In src/registry/bef_model_conversion.rs
impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Model construction from registry-specific columns
    }
}

// In src/models/individual.rs
impl Individual {
    // Only generic methods independent of registry format
}
```

### Migration Steps

1. **Move Registry-Specific Logic**:
   - Move registry-specific conversion methods from model files to registry conversion files

2. **Implement Registry Traits**:
   - Implement appropriate registry traits like `BefRegistry`, `IndRegistry`, etc.

3. **Remove Direct Dependencies**:
   - Remove direct registry references from model implementations

## Migrating Filtering Code

### Old Pattern

```rust
// Old pattern with custom filter application
let filtered_data = load_parquet_with_expr_filter(path, &filter_expr);
```

### New Pattern

```rust
// New pattern with trait-based filtering
let loader = ParquetLoader::with_schema_ref(schema);
let filtered_data = loader.load_with_expr_async(path, &filter_expr).await?;
```

### Migration Steps

1. **Create Appropriate Loader**:
   - Create a loader with the correct schema: `ParquetLoader::with_schema_ref(schema)`

2. **Use Trait Methods**:
   - Use `load_with_expr_async` for expression-based filtering
   - Use `load_with_filter_async` for custom filter implementations
   - Use `load_with_pnr_filter_async` for PNR-based filtering

## Code Removal Safety

When removing old code, follow these guidelines:

1. **Validate First**: Ensure the new implementations match the old behavior with tests
2. **Deprecate Before Removing**: Mark old functions as deprecated for a transitional period
3. **Document Migration Path**: Add comments directing users to the new APIs
4. **Update All Usages**: Search for all usage sites and update them before removal

## Examples

See the following examples for complete migration patterns:

- `src/registry/bef_migration_example.rs` - BEF registry migration
- `src/registry/ind_migration_example.rs` - IND registry with year filtering

## FAQ

**Q: Does this change the external API of registries?**  
A: No, the external `RegisterLoader` trait methods remain unchanged. All changes are internal implementation details.

**Q: Do I need to change how I use registries in my code?**  
A: No, existing code using the `RegisterLoader` trait should continue to work without changes.

**Q: Is there a performance impact?**  
A: The new trait-based system should have similar or better performance, especially for complex operations.

**Q: How do I add custom filtering?**  
A: You can chain multiple filters using the `FilterBuilder` or implement custom `BatchFilter` implementations.

## Timeline

1. **Phase 1 (Current)**: Create template implementations and migration guide
2. **Phase 2 (Next 2 weeks)**: Migrate all registry implementations
3. **Phase 3 (Next 4 weeks)**: Remove legacy code and standardize patterns