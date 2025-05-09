/// Main test module that includes all sub-modules
/// Run specific tests with `cargo test <module>::<submodule>`
/// For example: `cargo test integration::filtering_test`
// Collection tests
pub mod collections;

// Model tests
pub mod models;

// Register tests
pub mod registry;

// Filter tests
pub mod filter;

// Integration tests
pub mod integration;

// Test utils
pub mod utils;
