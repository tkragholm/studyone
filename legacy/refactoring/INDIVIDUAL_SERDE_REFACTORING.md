# Individual Model Serde Refactoring

This document describes the refactoring of the Individual model's serde functionality to eliminate duplication between the base Individual model and the serde-enhanced version.

## Problem Statement

We identified two key issues with the current implementation:

1. **Duplicate struct definitions**: The Individual struct was defined in both `base.rs` and `serde.rs`, leading to maintenance challenges when fields need to be updated.

2. **Duplicate type conversion code**: The serde module implemented its own match expressions for converting registry values to enum types, duplicating the conversion logic already defined in the `types.rs` module.

## Solution

The solution implements a wrapper-based approach to serde that leverages existing conversion functionality:

1. **Use From implementations from types.rs**: Custom deserializers now use the existing `From` trait implementations from the types module, eliminating redundant match arms.

2. **Create SerdeIndividual as a wrapper**: Rather than duplicating the struct definition, we've created a newtype wrapper that uses the `#[serde(with = "...")]` pattern to apply serde attributes while reusing the base Individual definition.

3. **Use serde remote attribute**: We define the deserialization mapping in a remote struct (`IndividualDef`) that adds serde field attributes while referencing the core Individual type.

4. **Compute derived fields during deserialization**: The is_rural field is computed after deserialization using the existing compute_rural_status method.

## Key Components

### 1. IndividualDef Struct

This struct defines the serde mapping attributes to allow direct deserialization from registry field names to Individual fields:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "Individual")]
struct IndividualDef {
    /// Personal identification number (PNR)
    #[serde(alias = "PNR")]
    pnr: String,
    
    /// Gender of the individual
    #[serde(alias = "KOEN", deserialize_with = "deserialize_gender")]
    gender: Gender,
    
    // ... other fields with serde attributes ...
}
```

### 2. SerdeIndividual Wrapper

This wrapper holds an inner Individual and provides methods to access and convert it:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerdeIndividual {
    /// The inner Individual that holds the actual data
    #[serde(with = "IndividualDef")]
    inner: Individual,
}
```

### 3. Custom Deserializer Functions

These functions now use the existing From implementations:

```rust
fn deserialize_gender<'de, D>(deserializer: D) -> std::result::Result<Gender, D::Error>
where
    D: Deserializer<'de>,
{
    let gender_code = String::deserialize(deserializer)?;
    Ok(Gender::from(gender_code.as_str()))
}
```

### 4. Conversion Methods

The SerdeIndividual provides methods to convert between standard Individual and SerdeIndividual:

```rust
impl SerdeIndividual {
    /// Get reference to the underlying Individual
    pub fn inner(&self) -> &Individual {
        &self.inner
    }
    
    /// Convert into the inner Individual
    pub fn into_inner(self) -> Individual {
        self.inner
    }
    
    /// Convert from the standard Individual model
    pub fn from_standard(standard: &Individual) -> Self {
        Self { inner: standard.clone() }
    }
}
```

## Registry Conversion Integration

The registry conversion code has been updated to use the new SerdeIndividual wrapper:

```rust
pub fn convert_batch_with_serde_arrow(batch: &RecordBatch) -> Result<Vec<Individual>> {
    // Using direct serde_arrow conversion with SerdeIndividual
    match SerdeIndividual::from_batch(batch) {
        Ok(serde_individuals) => {
            // Convert from SerdeIndividual to standard Individual
            let individuals = serde_individuals.into_iter()
                .map(|serde_ind| serde_ind.into_inner())
                .collect();
            Ok(individuals)
        },
        Err(e) => Err(anyhow::anyhow!("Serde Arrow deserialization error: {}", e)),
    }
}
```

## Benefits

1. **Single source of truth**: The Individual struct is defined only once in base.rs.
2. **Reuse of conversion logic**: We leverage the existing From implementations for enum conversions.
3. **Clear separation of concerns**: The serde functionality is now clearly separated from the core model.
4. **Improved maintainability**: Changes to the Individual struct only need to be made in one place.
5. **Type safety**: The wrapper approach provides type safety while maintaining ease of conversion.

## Next Steps

1. Apply the same pattern to other models like Family, Diagnosis, etc.
2. Update registry conversion implementations to use the new SerdeIndividual wrapper.
3. Consider adding property tests to verify that serialization and deserialization are symmetrical.