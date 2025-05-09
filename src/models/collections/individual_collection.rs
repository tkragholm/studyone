//! Individual collection implementation
//!
//! This module provides a collection type for storing and querying
//! Individual models efficiently.
//!
//! DEPRECATED: This implementation is deprecated in favor of the
//! implementation in `src/collections/individual.rs`. This module
//! now re-exports that implementation for backward compatibility.

#[deprecated(
    since = "0.2.0",
    note = "Use the IndividualCollection from src/collections/individual.rs instead. This will be removed in a future version."
)]
pub use crate::collections::IndividualCollection;

// Note: All code has been moved to the canonical implementation in
// src/collections/individual.rs. This re-export is maintained for
// backward compatibility only.
