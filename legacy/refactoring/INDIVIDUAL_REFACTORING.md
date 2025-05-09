# Individual Model Refactoring

This document describes the refactoring of the Individual model to make it more modular and maintainable.

## Problem

The original Individual model was defined in a single, large file with over 900 lines of code. This file contained:

1. The base Individual struct definition
2. Core methods and implementations
3. Time-related functionality (age calculation, validity)
4. Family relationship methods
5. Registry data enhancement
6. Serialization and deserialization logic

In addition, a separate serde-enhanced version was created in `individual_serde.rs`, which used serde attributes for registry field mapping.

## Solution: Module-Based Approach

We refactored the Individual model into a modular structure with clear separation of concerns:

```
models/core/individual/
├── mod.rs              # Main exports and module organization
├── base.rs             # Core struct definition and basic methods
├── relationships.rs    # Family relationships and derived models
├── temporal.rs         # Time-related functionality (age, validity)
├── registry.rs         # Registry data enhancement
├── conversion.rs       # Arrow schema conversion
├── serde.rs            # Serde-enhanced version with field mapping
└── tests.rs            # Tests (only in test builds)
```

## Benefits

1. **Improved Maintainability**: Each aspect of the Individual model is now in its own file with a clear, focused responsibility.

2. **Better Organization**: The code is logically organized by functionality, making it easier to find and modify specific features.

3. **Reduced File Size**: Instead of a single 900+ line file, we now have multiple smaller files that are easier to understand.

4. **Clearer Boundaries**: Each module has a well-defined responsibility, reducing interdependencies.

5. **Consolidated Implementations**: The serde-enhanced version is now part of the overall structure with conversion methods between the two approaches.

## Usage Examples

### Basic Individual Creation

```rust
use models::core::Individual;

let individual = Individual::new(
    "1234567890".to_string(),
    Gender::Male,
    Some(NaiveDate::from_ymd_opt(1980, 1, 1).unwrap())
);
```

### Using Serde-Enhanced Version for Registry Data

```rust
use models::core::individual::serde::Individual as SerdeIndividual;

// Convert directly from registry data
let individuals = SerdeIndividual::from_batch(&batch)?;

// Convert to standard Individual if needed
let standard_individuals = individuals.iter()
    .map(|i| i.to_standard())
    .collect::<Vec<_>>();
```

### Working with Relationships

```rust
// Creating family relationships
let families = Individual::create_families(&individuals, &reference_date);

// Finding children
let children = Individual::create_children(&individuals, &reference_date);
```

## Migration Path

For existing code that uses the old Individual structure:

1. All public methods and fields have been preserved with the same signatures
2. Import statements should be updated to use the new module path
3. For code using the serde-enhanced version, use the conversion methods

### Old Code:
```rust
use crate::models::Individual;
// or
use crate::models::individual_serde::Individual;
```

### New Code:
```rust
use crate::models::core::Individual;
// or
use crate::models::core::individual::serde::Individual;
```

## Future Improvements

1. Further reduce duplication between standard and serde-enhanced versions
2. Implement full trait coverage for the serde-enhanced version
3. Add more comprehensive tests for each module
4. Consider using macros to reduce code duplication in field initialization and copying