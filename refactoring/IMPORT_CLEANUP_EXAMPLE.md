# Example: Cleaning Up Redundant Imports

This document shows examples of how to remove redundant imports from model files as part of our refactoring effort.

## Individual Model Cleanup

### Before

```rust
// src/models/individual.rs
use crate::common::traits::{BefRegistry, DodRegistry, IndRegistry, RegistryAware};
use crate::error::Result;
use crate::models::traits::{ArrowSchema, EntityModel, HealthStatus, TemporalValidity};
use crate::models::types::{EducationLevel, Gender, Origin};
use crate::utils::array_utils::{downcast_array, get_column};
use arrow::array::{Array, BooleanArray, Date32Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Individual {
    pub pnr: String,
    pub gender: Gender,
    pub birth_date: Option<NaiveDate>,
    // ... other fields ...
}

impl Individual {
    // Core entity methods
    
    // Registry-specific methods
    pub fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Implementation that uses BEF-specific column names
    }
    
    pub fn from_ind_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Implementation that uses IND-specific column names
    }
}

// Registry trait implementations
impl RegistryAware for Individual {
    fn registry_name() -> &'static str {
        "Individual"
    }
    
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Multi-registry implementation
    }
    
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Batch conversion logic
    }
}

impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        Self::from_bef_record(batch, row)
    }
    
    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // BEF batch conversion
    }
}

impl IndRegistry for Individual {
    // Similar implementations
}
```

### After

```rust
// src/models/individual.rs
use crate::error::Result;
use crate::models::traits::{EntityModel, HealthStatus, TemporalValidity};
use crate::models::types::{EducationLevel, Gender, Origin};
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Individual {
    pub pnr: String,
    pub gender: Gender,
    pub birth_date: Option<NaiveDate>,
    // ... other fields ...
}

impl Individual {
    // Only core entity methods remain
    // Registry-specific methods are moved to registry implementations
    
    pub fn new(
        pnr: String,
        gender: Gender,
        birth_date: Option<NaiveDate>,
        // ... other parameters ...
    ) -> Self {
        Self {
            pnr,
            gender,
            birth_date,
            // ... other fields ...
        }
    }
    
    // Utility methods that don't depend on registry knowledge
}

// Core trait implementations only
impl EntityModel for Individual {
    type Id = String;
    
    fn id(&self) -> &Self::Id {
        &self.pnr
    }
    
    fn key(&self) -> String {
        self.pnr.clone()
    }
}

impl HealthStatus for Individual {
    // Implementation
}

impl TemporalValidity for Individual {
    // Implementation
}

// The registry-specific traits move to registry-specific files
```

## Registry Implementation Files

### New Registry-Specific Trait Implementation

```rust
// src/registry/bef_model_conversion.rs
use crate::RecordBatch;
use crate::error::Result;
use crate::models::Individual;
use crate::common::traits::BefRegistry;
use arrow::array::{Array, BooleanArray, Date32Array, Int32Array, StringArray};
use crate::utils::array_utils::{downcast_array, get_column};

// Move registry-specific trait implementation here
impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Implement conversion from BEF registry format
        // This was previously in Individual::from_bef_record
    }
    
    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Implement batch conversion
    }
}
```

## Steps to Clean Up Imports

1. **Identify Registry-Specific Imports**:
   - Look for imports like `BefRegistry`, `DodRegistry`, etc.
   - Look for Arrow-specific imports that are only used in registry conversion

2. **Remove Registry-Specific Methods**:
   - Move methods like `from_bef_record` to registry-specific files
   - Keep only core entity behavior in the model file

3. **Remove Registry Trait Implementations**:
   - Move trait implementations like `impl BefRegistry` to registry files

4. **Update References**:
   - Update any places that called the removed methods to use the trait implementation instead
   - For example, change `Individual::from_bef_record(batch, row)` to `Individual::from_bef_record(batch, row)`

5. **Clean Up Unused Imports**:
   - Remove any imports that are no longer needed after the methods are moved

## Benefits

- Clear separation of concerns
- Models focus on their core functionality
- Registry-specific knowledge stays in registry files
- Reduced coupling between modules
- Better maintainability and testability