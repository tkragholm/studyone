//! LPR (Landspatientregistret) registry loaders
//!
//! This module contains registry loaders for different versions of the Danish National Patient Registry (LPR).

// Re-export specific loaders
pub use self::lpr2::{LprAdmRegister, LprDiagRegister, LprBesRegister};
pub use self::lpr3::{Lpr3KontakterRegister, Lpr3DiagnoserRegister};
pub use self::discovery::{LprPaths, find_lpr_files};

// Import submodules
pub mod lpr2;
pub mod lpr3;
pub mod discovery;