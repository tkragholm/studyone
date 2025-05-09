# Module Restructuring Plan

## Current Structure

Currently, the models are organized as flat sibling modules:

```
src/models/
├── individual.rs     # Core individual model
├── child.rs          # Child model wrapping Individual
├── parent.rs         # Parent model wrapping Individual
├── family.rs         # Family model composed of Individuals
├── diagnosis.rs      # Health diagnosis data
├── income.rs         # Income data
├── collections.rs    # Collection utilities
├── traits.rs         # Shared traits
└── types.rs          # Shared type definitions
```

## Proposed Structure

We'll restructure the modules to reflect the hierarchical nature of the models:

```
src/models/
├── core/                     # Core entities
│   ├── mod.rs                # Module exports
│   ├── individual.rs         # Core individual model (foundation)
│   ├── types.rs              # Shared type definitions
│   └── traits.rs             # Core model traits
│
├── derived/                  # Models derived from Individual
│   ├── mod.rs                # Module exports
│   ├── child.rs              # Child model (derived from Individual)
│   ├── parent.rs             # Parent model (derived from Individual)
│   └── family.rs             # Family model (composed of Individuals)
│
├── health/                   # Health-related models
│   ├── mod.rs                # Module exports
│   └── diagnosis.rs          # Diagnosis model
│
├── economic/                 # Economic-related models
│   ├── mod.rs                # Module exports
│   └── income.rs             # Income model
│
├── collections/              # Collection utilities
│   ├── mod.rs                # Module exports
│   └── collection_traits.rs  # Collection-related traits
│
└── mod.rs                    # Main module exports (public re-exports)
```

## Benefits of New Structure

1. **Hierarchical Organization**: Clearly shows which models are fundamental and which are derived
2. **Domain Separation**: Groups models by their domain context (core, health, economic)
3. **Improved Discoverability**: Makes it easier to find related models
4. **Cleaner Imports**: Allows for more intuitive import paths (e.g., `models::core::Individual`)
5. **Better Extensibility**: Easier to add new model categories in the future

## Implementation Steps

1. Create the new directory structure
2. Move existing files to their new locations
3. Update module declarations in mod.rs files
4. Update imports in all files to reflect new paths
5. Ensure re-exports in the main mod.rs maintain backward compatibility
6. Update tests to use the new import paths

## Import Example

Before:
```rust
use crate::models::individual::Individual;
use crate::models::child::Child;
use crate::models::types::Gender;
```

After:
```rust
use crate::models::core::Individual;
use crate::models::derived::Child;
use crate::models::core::types::Gender;
```

With re-exports for backward compatibility:
```rust
// src/models/mod.rs
pub use core::Individual;
pub use derived::{Child, Parent, Family};
pub use core::types::{Gender, Origin, /* etc */};
```

This allows both new hierarchical imports and maintains compatibility with existing code.