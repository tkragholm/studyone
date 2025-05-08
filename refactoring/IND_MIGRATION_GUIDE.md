# IND Registry Migration Guide

This document details the process and implementation patterns used to migrate the IND (Indkomst) registry to the new trait-based approach. Use this as a reference for migrating other registries.

## Key Components Updated

1. **registry/ind/mod.rs**
   - Updated to use the trait-based approach with PnrFilterableLoader
   - Added year-specific filtering functionality
   - Implemented comprehensive tests for filtering

2. **registry/ind/conversion.rs**
   - Removed duplicate IndRegistry trait implementation
   - Preserved YearConfiguredIndRegister implementation
   - Used model-implemented trait methods to delegate conversion logic

## Implementation Pattern

### 1. Register Loader Structure

```rust
pub struct IndRegister {
    schema: SchemaRef,
    loader: Arc<PnrFilterableLoader>,  // Use the trait-based loader
    year: Option<i32>,                 // Optional registry-specific configuration
}
```

### 2. Constructor Pattern

```rust
impl IndRegister {
    pub fn new() -> Self {
        let schema = schema::ind_schema();
        let loader = PnrFilterableLoader::with_schema_ref(schema.clone())
            .with_pnr_column("PNR");
        
        Self {
            schema,
            loader: Arc::new(loader),
            year: None,
        }
    }
    
    // Registry-specific constructor variants as needed
    pub fn for_year(year: i32) -> Self {
        // Similar to new() but with year set
    }
}
```

### 3. RegisterLoader Trait Implementation

```rust
impl RegisterLoader for IndRegister {
    fn get_register_name(&self) -> &'static str {
        "IND"
    }

    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn load(&self, base_path: &Path, pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        // Create runtime for async code
        let rt = tokio::runtime::Runtime::new()?;
        
        // Execute async loader with appropriate filter
        rt.block_on(async {
            let result = if let Some(filter) = pnr_filter {
                self.loader.load_with_pnr_filter_async(base_path, Some(filter)).await?
            } else {
                self.loader.load_directory_async(base_path).await?
            };
            
            // Apply additional registry-specific filtering
            // ...
            
            Ok(result)
        })
    }
    
    fn load_async<'a>(&'a self, base_path: &'a Path, pnr_filter: Option<&'a HashSet<String>>)
        -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // Similar to load() but returns a future directly
    }
    
    fn supports_pnr_filter(&self) -> bool {
        true  // Or false if PNR filtering isn't supported
    }
    
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("PNR")  // Or None if no direct PNR column
    }
}
```

### 4. ModelConversion Implementation

```rust
impl ModelConversion<ModelType> for IndRegister {
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<ModelType>> {
        // Typically delegates to the model's registry trait implementation
        use crate::common::traits::IndRegistry;
        ModelType::from_ind_batch(batch)
    }
    
    fn from_models(&self, models: &[ModelType]) -> Result<RecordBatch> {
        // Implementation or error for now
        Err(anyhow::anyhow!("Not yet implemented"))
    }
    
    fn transform_models(&self, models: &mut [ModelType]) -> Result<()> {
        // Apply any registry-specific transformations
        Ok(())
    }
}
```

### 5. Testing Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_registry_functionality() -> Result<()> {
        let register = IndRegister::new();
        let test_path = PathBuf::from("test_data/ind");
        
        // Test loading
        let result = register.load_async(&test_path, None).await?;
        
        // Assertions
        assert!(!result.is_empty());
        
        Ok(())
    }
}
```

## Common Issues and Solutions

1. **Duplicate Trait Implementations**
   - Problem: IndRegistry was implemented for Individual in both models/individual.rs and registry/ind/conversion.rs
   - Solution: Keep trait implementations in a single location (typically in the model files)
   - Fix: Remove the duplicate implementation and add a comment indicating where the actual implementation resides

2. **Type Conversion Issues**
   - Problem: LiteralValue does not implement From<i32>
   - Solution: Use explicit conversion: LiteralValue::Int(i64::from(year))
   - Context: This occurs in filter expressions: Expr::Eq("YEAR".to_string(), LiteralValue::Int(i64::from(year)))

3. **Unused Imports**
   - Problem: After refactoring, some imports become unused
   - Solution: Regularly run `cargo check` and clean up unused imports

## Migration Steps for Other Registries

1. **Analysis**
   - Examine the current implementation
   - Identify registry-specific features that need to be preserved
   - Check for existing trait implementations in model files

2. **Implementation**
   - Update mod.rs with trait-based loader
   - Keep or update registry-specific functionality
   - Update conversion.rs to use model-implemented traits
   - Remove any duplicate trait implementations

3. **Testing**
   - Implement tests for the new registry loader
   - Verify loading and filtering functionality
   - Check compatibility with existing code

4. **Integration**
   - Update imports in files that reference the registry
   - Run cargo check to ensure everything compiles
   - Run tests to verify functionality

## Next Registry: MFR

The MFR (Medical Birth Registry) requires similar updates:
- Update registry/mfr/mod.rs with PnrFilterableLoader
- Preserve MfrChildRegister functionality
- Ensure proper trait implementations
- Add comprehensive tests