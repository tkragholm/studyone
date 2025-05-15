//! AKM registry using the macro-based approach
//!
//! The AKM (Arbejdsklassifikationsmodulet) registry contains employment information.

use crate::RegistryTrait;
use crate::common::traits::async_loading::AsyncFilterableLoader;
use crate::common::traits::async_loading::AsyncLoader;

/// Labour register with employment information
#[derive(RegistryTrait, Debug)]
#[registry(name = "AKM", description = "Labour register")]
pub struct AkmRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Socioeconomic status code
    #[field(name = "SOCIO13")]
    pub socioeconomic_status: Option<String>,
}

/// Helper function to create a new AKM deserializer
pub fn create_deserializer() -> AkmRegistryDeserializer {
    AkmRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &AkmRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for AkmRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "AKM"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for AKM
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("SOCIO13", arrow::datatypes::DataType::Utf8, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }

    /// Enable or disable the unified schema system
    fn use_unified_system(&mut self, _enable: bool) {
        // Always using unified system, no-op
    }

    /// Check if the unified schema system is enabled
    fn is_unified_system_enabled(&self) -> bool {
        true // Always enabled
    }

    /// Load records from the AKM register
    fn load(
        &self,
        base_path: &std::path::Path,
        pnr_filter: Option<&std::collections::HashSet<String>>,
    ) -> crate::Result<Vec<crate::RecordBatch>> {
        // Create a loader with our schema
        let schema = self.get_schema();
        let loader =
            crate::async_io::loader::Loader::with_schema_ref(schema).with_pnr_column("PNR");

        // Create a blocking runtime to run the async code
        let rt = tokio::runtime::Runtime::new()?;

        // Use the trait implementation to load data
        rt.block_on(async {
            if let Some(filter) = pnr_filter {
                // Create a PNR filter using the expr module
                use crate::filter::expr::{Expr, ExpressionFilter, LiteralValue};

                // Create the expression filter using the proper column name
                let values: Vec<LiteralValue> = filter
                    .iter()
                    .map(|s| LiteralValue::String(s.clone()))
                    .collect();

                let expr = Expr::In("PNR".to_string(), values);
                let pnr_filter = ExpressionFilter::new(expr);

                // Use filter with loader
                loader
                    .load_with_filter_async(base_path, std::sync::Arc::new(pnr_filter))
                    .await
            } else {
                // Otherwise use the directory loader
                loader.load_async(base_path).await
            }
        })
    }

    /// Load records from the AKM register asynchronously
    fn load_async<'a>(
        &'a self,
        base_path: &'a std::path::Path,
        pnr_filter: Option<&'a std::collections::HashSet<String>>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::Result<Vec<crate::RecordBatch>>> + Send + 'a>,
    > {
        // Get the schema and clone other needed values to move into the async block
        let schema = self.get_schema();

        // Move everything into the async block to avoid local variable references
        Box::pin(async move {
            // Create a loader inside the async block
            let loader = crate::async_io::loader::Loader::with_schema_ref(schema.clone())
                .with_pnr_column("PNR");

            if let Some(filter) = pnr_filter {
                // Create a PNR filter using the expr module
                use crate::filter::expr::{Expr, ExpressionFilter, LiteralValue};

                // Create the expression filter using the proper column name
                let values: Vec<LiteralValue> = filter
                    .iter()
                    .map(|s| LiteralValue::String(s.clone()))
                    .collect();

                let expr = Expr::In("PNR".to_string(), values);
                let pnr_filter = ExpressionFilter::new(expr);

                // Use filter with loader
                loader
                    .load_with_filter_async(base_path, std::sync::Arc::new(pnr_filter))
                    .await
            } else {
                // Otherwise use the directory loader
                loader.load_async(base_path).await
            }
        })
    }

    /// Returns whether this registry supports direct PNR filtering
    fn supports_pnr_filter(&self) -> bool {
        true
    }

    /// Returns the column name containing the PNR
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("PNR")
    }
}
