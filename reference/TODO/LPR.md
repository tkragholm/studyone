# LPR Module Refactoring Plan

The current `lpr.rs` file contains several distinct components that should be separated for better maintainability and organization.

## Current Structure

The `lpr.rs` file currently contains:
1. Common code for all LPR register loaders
2. LPR2-specific loaders (LprAdmRegister, LprDiagRegister, LprBesRegister)
3. LPR3-specific loaders (Lpr3KontakterRegister, Lpr3DiagnoserRegister)
4. File discovery utilities (LprPaths, find_lpr_files, visit_dirs)

## Proposed Refactoring

Restructure into the following modules:

1. `src/registry/lpr/mod.rs` - Main module with common utilities and reexports
2. `src/registry/lpr/lpr2.rs` - LPR2-specific register loaders
3. `src/registry/lpr/lpr3.rs` - LPR3-specific register loaders  
4. `src/registry/lpr/discovery.rs` - File discovery utilities

## Implementation Details

### 1. src/registry/lpr/mod.rs

This file would serve as the entrypoint for the LPR module, containing common imports, type definitions, and reexports:

```rust
//! LPR (Landspatientregistret) registry loaders
//!
//! This module contains registry loaders for different versions of the Danish National Patient Registry (LPR).

// Re-export specific loaders
pub use self::lpr2::{LprAdmRegister, LprDiagRegister, LprBesRegister};
pub use self::lpr3::{Lpr3KontakterRegister, Lpr3DiagnoserRegister};
pub use self::discovery::{LprPaths, find_lpr_files};

// Import submodules
mod lpr2;
mod lpr3;
mod discovery;
```

### 2. src/registry/lpr/lpr2.rs

This file would contain LPR2-specific register loaders:

```rust
//! LPR2 registry loaders
//!
//! This module contains registry loaders for LPR2 (Danish National Patient Registry version 2).

use crate::registry::RegisterLoader;
use crate::registry::schemas::lpr_adm::lpr_adm_schema;
use crate::registry::schemas::lpr_bes::lpr_bes_schema;
use crate::registry::schemas::lpr_diag::lpr_diag_schema;
use crate::Error;
use crate::RecordBatch;
use crate::Result;

use crate::load_parquet_files_parallel;
use crate::async_io::parallel_ops::load_parquet_files_parallel_with_pnr_filter_async;
use crate::read_parquet;
use crate::async_io::filter_ops::read_parquet_with_optional_pnr_filter_async;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

// Implementation of LprAdmRegister, LprDiagRegister, LprBesRegister
```

### 3. src/registry/lpr/lpr3.rs

This file would contain LPR3-specific register loaders:

```rust
//! LPR3 registry loaders
//!
//! This module contains registry loaders for LPR3 (Danish National Patient Registry version 3).

use crate::registry::RegisterLoader;
use crate::registry::schemas::lpr3_diagnoser::lpr3_diagnoser_schema;
use crate::registry::schemas::lpr3_kontakter::lpr3_kontakter_schema;
use crate::Error;
use crate::RecordBatch;
use crate::Result;

use crate::load_parquet_files_parallel;
use crate::async_io::parallel_ops::load_parquet_files_parallel_with_pnr_filter_async;
use crate::read_parquet;
use crate::async_io::filter_ops::read_parquet_with_optional_pnr_filter_async;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

// Implementation of Lpr3KontakterRegister, Lpr3DiagnoserRegister
```

### 4. src/registry/lpr/discovery.rs

This file would contain file discovery utilities:

```rust
//! LPR file discovery utilities
//!
//! This module contains utilities for discovering LPR files in file systems.

use crate::Error;
use crate::Result;
use std::path::{Path, PathBuf};

// Implementation of LprPaths, find_lpr_files, visit_dirs
```

## Benefits of This Refactoring

1. **Separation of Concerns**: Each file has a clear, focused purpose
2. **Improved Maintainability**: Smaller files are easier to understand and modify
3. **Better Organization**: Code is grouped by logical function instead of being in one large file
4. **Consistent with Schema Structure**: Follows the same pattern as the schemas directory
5. **Easier Navigation**: Developers can quickly find relevant code
6. **Reduced Merge Conflicts**: Team members working on different aspects are less likely to conflict