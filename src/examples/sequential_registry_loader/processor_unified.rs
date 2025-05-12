use crate::error::Result;
use crate::examples::parrallel_loader::load_parquet_files_parallel_with_filter;
use crate::filter_expression::{Expr, col};
use crate::models::Individual;
use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Import the unified system components
use crate::registry::akm::schema_unified;
use crate::registry::generic_deserializer::GenericDeserializer;
use crate::schema::field_def::SchemaAdapter;

/// Processes registry data in a sequential, interdependent manner using the unified system,
/// following the natural data dependencies:
/// 1. Load BEF to identify children by birth date
/// 2. Load MFR and link with children from BEF
/// 3. Add death and migration events from DOD and VNDS
/// 4. Enrich with socioeconomic data from AKM, UDDF, IND
pub struct UnifiedRegistryProcessor {
    /// Base directory containing registry data
    base_dir: Arc<Path>,
    /// Date ranges for filtering
    start_date: String,
    end_date: String,
    /// Individuals loaded from BEF registry
    individuals: HashMap<String, Individual>,
    /// Statistics collected during processing
    stats: HashMap<String, usize>,
    /// IDs of individuals of interest (valid PNRs)
    valid_pnrs: HashSet<String>,
    /// Parent-child relationships (`child_pnr` -> (`mother_pnr`, `father_pnr`))
    relationships: HashMap<String, (Option<String>, Option<String>)>,
    /// Schema adapter for unified type adaption
    schema_adapter: SchemaAdapter,
}

impl UnifiedRegistryProcessor {
    /// Create a new processor with the specified date range
    #[must_use]
    pub fn new(base_dir: &Path, start_date: &str, end_date: &str) -> Self {
        Self {
            base_dir: Arc::from(base_dir),
            start_date: start_date.to_string(),
            end_date: end_date.to_string(),
            individuals: HashMap::new(),
            stats: HashMap::new(),
            valid_pnrs: HashSet::new(),
            relationships: HashMap::new(),
            schema_adapter: SchemaAdapter::default(),
        }
    }

    /// Get the full path to a registry directory
    fn registry_path(&self, registry: &str) -> PathBuf {
        self.base_dir.join(registry.to_lowercase())
    }

    /// Process all registries in the correct sequence
    pub async fn process_all(&mut self) -> Result<()> {
        // Step 1: Load BEF data to identify children by birth date
        self.load_bef_registry().await?;

        // Step 2: Load MFR for additional birth information
        self.load_mfr_registry().await?;

        // Step 3: Load DOD and VNDS for mortality and migration
        self.load_death_registry().await?;
        self.load_migration_registry().await?;

        // Step 4: Load socioeconomic data from AKM, UDDF, IND
        self.load_employment_registry().await?;
        self.load_education_registry().await?;
        self.load_income_registry().await?;

        // Record final statistics
        self.stats
            .insert("total_individuals".into(), self.individuals.len());

        Ok(())
    }

    /// Step 1: Load BEF registry and identify children by birth date
    async fn load_bef_registry(&mut self) -> Result<()> {
        let registry_dir = self.registry_path("BEF");
        if !registry_dir.exists() || !registry_dir.is_dir() {
            warn!(
                "BEF registry directory not found: {}",
                registry_dir.display()
            );
            return Ok(());
        }

        info!(
            "Loading BEF data to identify children born between {} and {}",
            self.start_date, self.end_date
        );

        // BEF columns needed for basic demographic information
        let columns = vec![
            "PNR".to_string(),
            "FOED_DAG".to_string(),
            "KOEN".to_string(),
            "FAR_ID".to_string(),
            "MOR_ID".to_string(),
            "FAMILIE_ID".to_string(),
            "STATSB".to_string(),
            "CIVST".to_string(),
        ];

        // Filter for births within our date range
        let start_date = self.start_date.clone();
        let end_date = self.end_date.clone();

        // This is a workaround since we're using string dates
        // For real date types, we should use proper date comparison
        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &col("FOED_DAG").contains(""), // Get all non-null birth dates first
            Some(&columns),
        )?;

        // Filter and build the initial set of individuals
        let mut initial_count = 0;
        let mut batch_count = 0;

        // Create a BEF deserializer using the unified system
        // Note: We'll need to create a BEF schema definition using the unified system
        // For this example, we'll use the AKM schema as a placeholder
        let bef_schema = schema_unified::create_akm_schema();
        let deserializer = GenericDeserializer::new(bef_schema);

        for batch in &record_batches {
            batch_count += 1;
            let filtered_batch = self.filter_batch_by_birth_date(batch, &start_date, &end_date)?;

            if let Some(filtered) = filtered_batch {
                // Convert to Individual models using the unified deserializer
                if let Ok(individuals) = deserializer.deserialize_batch(&filtered) {
                    for individual in individuals {
                        let pnr = individual.pnr.clone();

                        // Store parent-child relationships
                        self.relationships.insert(
                            pnr.clone(),
                            (individual.mother_pnr.clone(), individual.father_pnr.clone()),
                        );

                        // Add to valid PNRs
                        self.valid_pnrs.insert(pnr.clone());

                        // Store the individual
                        self.individuals.insert(pnr, individual);
                        initial_count += 1;
                    }
                }
            }
        }

        info!("Identified {initial_count} individuals born between {start_date} and {end_date}");

        // Record statistics
        self.stats.insert("bef_total_batches".into(), batch_count);
        self.stats.insert("bef_individuals".into(), initial_count);

        Ok(())
    }

    /// Filter a batch of records by birth date
    /// This is a helper function to filter BEF data by birth date
    fn filter_batch_by_birth_date(
        &self,
        batch: &RecordBatch,
        start_date: &str,
        end_date: &str,
    ) -> Result<Option<RecordBatch>> {
        // Get the birth date column
        let birth_date_col = match batch
            .column_by_name("FOED_DAG")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        {
            Some(col) => col,
            None => return Ok(None),
        };

        // Find rows where birth date is within range
        let mut indices = Vec::new();
        for row in 0..batch.num_rows() {
            if birth_date_col.is_null(row) {
                continue;
            }

            let birth_date = birth_date_col.value(row).to_string();
            if *birth_date >= *start_date && *birth_date <= *end_date {
                indices.push(row as u64);
            }
        }

        if indices.is_empty() {
            return Ok(None);
        }

        // Extract the matching rows
        use arrow::compute::take;
        let columns = batch
            .columns()
            .iter()
            .map(|col| take(col, &arrow::array::UInt64Array::from(indices.clone()), None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let filtered_batch = RecordBatch::try_new(batch.schema(), columns)?;
        Ok(Some(filtered_batch))
    }

    /// Step 2: Load MFR registry and link with children from BEF
    async fn load_mfr_registry(&mut self) -> Result<()> {
        let registry_dir = self.registry_path("MFR");
        if !registry_dir.exists() || !registry_dir.is_dir() {
            warn!(
                "MFR registry directory not found: {}",
                registry_dir.display()
            );
            return Ok(());
        }

        info!("Loading MFR data to enrich birth information");

        // MFR columns needed for birth details
        let columns = vec![
            "CPR_BARN".to_string(),             // Child's PNR
            "CPR_MODER".to_string(),            // Mother's PNR
            "CPR_FADER".to_string(),            // Father's PNR
            "FLERFOLDSGRAVIDITET".to_string(),  // Multiple birth indicator
            "GESTATIONSALDER_DAGE".to_string(), // Gestational age
            "VAEGT_BARN".to_string(),           // Birth weight
            "LAENGDE_BARN".to_string(),         // Birth length
        ];

        // For MFR, we filter using the child PNRs we already identified
        let child_pnrs = self.valid_pnrs.clone();

        // Create a filter expression that looks for CPR_BARN instead of PNR for MFR
        let pnr_filter_expr =
            col("CPR_BARN").in_list(child_pnrs.iter().cloned().collect::<Vec<_>>());

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Enrich individuals with MFR data
        let mut enriched_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the child's PNR from CPR_BARN column
                if let Some(pnr_col) = batch
                    .column_by_name("CPR_BARN")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                {
                    if pnr_col.is_null(row) {
                        continue;
                    }

                    let pnr = pnr_col.value(row).to_string();

                    // Enrich the individual if we found them in the BEF data
                    if let Some(individual) = self.individuals.get_mut(&pnr) {
                        // Extract other MFR data and add to the individual
                        // Birth weight
                        if let Some(weight_col) = batch
                            .column_by_name("VAEGT_BARN")
                            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                        {
                            if !weight_col.is_null(row) {
                                let weight_str = weight_col.value(row);
                                if let Ok(_weight) = weight_str.parse::<i32>() {
                                    // Store birth weight as a custom field since we can't modify the Individual directly
                                    // In a full implementation, you might use a custom structure with metadata
                                }
                            }
                        }

                        // Add mother's PNR if missing
                        if individual.mother_pnr.is_none() {
                            if let Some(mother_col) = batch
                                .column_by_name("CPR_MODER")
                                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                            {
                                if !mother_col.is_null(row) {
                                    let mother_pnr = mother_col.value(row).to_string();
                                    individual.mother_pnr = Some(mother_pnr);
                                }
                            }
                        }

                        // Add father's PNR if missing
                        if individual.father_pnr.is_none() {
                            if let Some(father_col) = batch
                                .column_by_name("CPR_FADER")
                                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                            {
                                if !father_col.is_null(row) {
                                    let father_pnr = father_col.value(row).to_string();
                                    individual.father_pnr = Some(father_pnr);
                                }
                            }
                        }

                        enriched_count += 1;
                    }
                }
            }
        }

        info!("Enriched {enriched_count} individuals with MFR data");

        // Record statistics
        self.stats.insert("mfr_enriched".into(), enriched_count);

        Ok(())
    }

    /// Step 3a: Load DOD registry for mortality information
    async fn load_death_registry(&mut self) -> Result<()> {
        let registry_dir = self.registry_path("DOD");
        if !registry_dir.exists() || !registry_dir.is_dir() {
            warn!(
                "DOD registry directory not found: {}",
                registry_dir.display()
            );
            return Ok(());
        }

        info!("Loading DOD data for mortality information");

        // DOD columns
        let columns = vec![
            "PNR".to_string(),     // Person ID
            "DODDATO".to_string(), // Date of death
        ];

        // Filter by our PNRs of interest
        let pnr_filter_expr = create_pnr_filter_expr(&self.valid_pnrs);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Add death information to individuals
        let mut death_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the PNR
                if let Some(pnr) = get_string_value(batch, "PNR", row) {
                    // Get the death date
                    if let Some(death_date) = get_string_value(batch, "DODDATO", row) {
                        // Add death date to the individual
                        if let Some(individual) = self.individuals.get_mut(&pnr) {
                            // In a real implementation, we'd convert this to a proper Date
                            // For simplicity, we're using strings in this example
                            individual.death_date = Some(
                                chrono::NaiveDate::parse_from_str(&death_date, "%Y%m%d")
                                    .unwrap_or_else(|_| {
                                        chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()
                                    }),
                            );
                            death_count += 1;
                        }
                    }
                }
            }
        }

        info!("Added death information to {death_count} individuals");

        // Record statistics
        self.stats.insert("dod_enriched".into(), death_count);

        Ok(())
    }

    /// Step 3b: Load VNDS registry for migration information
    async fn load_migration_registry(&mut self) -> Result<()> {
        let registry_dir = self.registry_path("VNDS");
        if !registry_dir.exists() || !registry_dir.is_dir() {
            warn!(
                "VNDS registry directory not found: {}",
                registry_dir.display()
            );
            return Ok(());
        }

        info!("Loading VNDS data for migration information");

        // VNDS columns
        let columns = vec![
            "PNR".to_string(),     // Person ID
            "UDRDTO".to_string(),  // Emigration date
            "INDRDTO".to_string(), // Immigration date
        ];

        // Filter by our PNRs of interest
        let pnr_filter_expr = create_pnr_filter_expr(&self.valid_pnrs);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Add migration information to individuals
        let mut migration_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the PNR
                if let Some(pnr) = get_string_value(batch, "PNR", row) {
                    // Check if we have migration dates
                    let emigration_date = get_string_value(batch, "UDRDTO", row);
                    let immigration_date = get_string_value(batch, "INDRDTO", row);

                    if emigration_date.is_some() || immigration_date.is_some() {
                        if let Some(individual) = self.individuals.get_mut(&pnr) {
                            // In a real implementation, we'd convert these to proper Dates
                            if let Some(date) = &emigration_date {
                                individual.emigration_date = Some(
                                    chrono::NaiveDate::parse_from_str(date, "%Y%m%d")
                                        .unwrap_or_else(|_| {
                                            chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()
                                        }),
                                );
                            }

                            if let Some(date) = &immigration_date {
                                individual.immigration_date = Some(
                                    chrono::NaiveDate::parse_from_str(date, "%Y%m%d")
                                        .unwrap_or_else(|_| {
                                            chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()
                                        }),
                                );
                            }

                            migration_count += 1;
                        }
                    }
                }
            }
        }

        info!("Added migration information to {migration_count} individuals");

        // Record statistics
        self.stats.insert("vnds_enriched".into(), migration_count);

        Ok(())
    }

    /// Step 4a: Load AKM registry for employment information
    async fn load_employment_registry(&mut self) -> Result<()> {
        let registry_dir = self.registry_path("AKM");
        if !registry_dir.exists() || !registry_dir.is_dir() {
            warn!(
                "AKM registry directory not found: {}",
                registry_dir.display()
            );
            return Ok(());
        }

        info!("Loading AKM data for employment information");

        // AKM columns
        let columns = vec![
            "PNR".to_string(),     // Person ID
            "SOCIO".to_string(),   // Socioeconomic status
            "SOCIO02".to_string(), // Socioeconomic status (2002 definition)
            "SOCIO13".to_string(), // Socioeconomic status (2013 definition)
            "SENR".to_string(),    // Workplace ID
        ];

        // Filter for all individuals of interest
        let pnr_filter_expr = create_pnr_filter_expr(&self.valid_pnrs);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Create a unified AKM deserializer
        let akm_schema = schema_unified::create_akm_schema();
        let deserializer = GenericDeserializer::new(akm_schema);

        // Add employment data to individuals
        let mut employment_count = 0;

        for batch in &record_batches {
            // Use the unified deserializer to get individuals
            if let Ok(akm_individuals) = deserializer.deserialize_batch(batch) {
                for akm_individual in akm_individuals {
                    let pnr = akm_individual.pnr.clone();

                    // Now handle parents that aren't in our individuals HashMap
                    if !self.individuals.contains_key(&pnr) {
                        // Add to individuals collection
                        self.individuals.insert(pnr.clone(), akm_individual);
                        self.valid_pnrs.insert(pnr.clone());
                        employment_count += 1;
                    } else {
                        // Update employment data for this individual
                        if let Some(individual) = self.individuals.get_mut(&pnr) {
                            // Update socioeconomic status if it's not unknown
                            if akm_individual.socioeconomic_status
                                != crate::models::core::types::SocioeconomicStatus::Unknown
                            {
                                individual.socioeconomic_status =
                                    akm_individual.socioeconomic_status;
                            }

                            // Update workplace ID if it's set
                            if akm_individual.workplace_id.is_some() {
                                individual.workplace_id = akm_individual.workplace_id;
                            }

                            employment_count += 1;
                        }
                    }
                }
            }
        }

        info!("Added employment information to {employment_count} individuals");

        // Record statistics
        self.stats.insert("akm_enriched".into(), employment_count);

        Ok(())
    }

    /// Remaining implementations for education_registry and income_registry would follow
    /// a similar pattern, but are omitted for brevity
    async fn load_education_registry(&mut self) -> Result<()> {
        info!("Education registry loading would use the unified system (omitted for brevity)");
        Ok(())
    }

    async fn load_income_registry(&mut self) -> Result<()> {
        info!("Income registry loading would use the unified system (omitted for brevity)");
        Ok(())
    }
}

/// Helper function to create a PNR filter expression
fn create_pnr_filter_expr(pnrs: &HashSet<String>) -> Expr {
    col("PNR").in_list(pnrs.iter().cloned().collect::<Vec<_>>())
}

/// Get a string value from a record batch column at the specified row index
#[must_use]
pub fn get_string_value(batch: &RecordBatch, column_name: &str, row: usize) -> Option<String> {
    batch
        .column_by_name(column_name)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .and_then(|array| {
            if row < array.len() && !array.is_null(row) {
                Some(array.value(row).to_string())
            } else {
                None
            }
        })
}

/// Run the sequential registry processor with unified system
///
/// This example shows how to process registry data in a sequential manner
/// using the unified data loading system.
///
/// # Arguments
/// * `base_path` - Base directory containing registry data
/// * `start_date` - Start date for filtering (in format 'YYYYMMDD')
/// * `end_date` - End date for filtering (in format 'YYYYMMDD')
///
/// # Returns
/// * `Result<()>` - Result indicating success or failure
pub async fn run_unified_registry_example(
    base_path: impl AsRef<Path>,
    start_date: &str,
    end_date: &str,
) -> Result<()> {
    // Create a new processor
    let mut processor = UnifiedRegistryProcessor::new(base_path.as_ref(), start_date, end_date);

    // Process all registries
    processor.process_all().await?;

    info!(
        "Processed {} individuals using unified system",
        processor.stats.get("total_individuals").unwrap_or(&0)
    );

    Ok(())
}
