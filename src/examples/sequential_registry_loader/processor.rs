use crate::error::Result;
use crate::examples::parrallel_loader::load_parquet_files_parallel_with_filter;
use crate::filter_expression::{Expr, col};
use crate::models::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::registry::bef;
use arrow::array::{Array, StringArray};
use arrow::compute::kernels::take;
use arrow::record_batch::RecordBatch;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::utils::map_socio_to_enum;

/// Processes registry data in a sequential, interdependent manner,
/// following the natural data dependencies:
/// 1. Load BEF to identify children by birth date
/// 2. Load MFR and link with children from BEF
/// 3. Add death and migration events from DOD and VNDS
/// 4. Enrich with socioeconomic data from AKM, UDDF, IND
pub struct SequentialRegistryProcessor {
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
}

impl SequentialRegistryProcessor {
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

        for batch in &record_batches {
            batch_count += 1;
            let filtered_batch = self.filter_batch_by_birth_date(batch, &start_date, &end_date)?;

            if let Some(filtered) = filtered_batch {
                // Convert to Individual models
                let field_mapping = bef::deserializer::field_mapping();
                let mapped_batch =
                    crate::registry::deserializer::create_mapped_batch(&filtered, field_mapping)?;

                if let Ok(serde_individuals) = SerdeIndividual::from_batch(&mapped_batch) {
                    for serde_ind in serde_individuals {
                        let individual = serde_ind.into_inner();
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
                                if let Ok(weight) = weight_str.parse::<i32>() {
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
        let valid_pnrs = self.valid_pnrs.clone();
        let pnr_filter_expr = create_pnr_filter_expr(&valid_pnrs);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Add death dates to individuals
        let mut death_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the PNR
                if let Some(pnr_col) = batch
                    .column_by_name("PNR")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                {
                    if pnr_col.is_null(row) {
                        continue;
                    }

                    let pnr = pnr_col.value(row).to_string();

                    // Set death date if we found the individual
                    if let Some(individual) = self.individuals.get_mut(&pnr) {
                        // Extract death date and set it
                        if let Some(date_col) = batch
                            .column_by_name("DODDATO")
                            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                        {
                            if !date_col.is_null(row) {
                                let death_date_str = date_col.value(row);
                                if let Ok(death_date) =
                                    chrono::NaiveDate::parse_from_str(death_date_str, "%Y-%m-%d")
                                {
                                    individual.death_date = Some(death_date);
                                    death_count += 1;
                                }
                            }
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
            "PNR".to_string(),        // Person ID
            "HAEND_DATO".to_string(), // Event date (used for both immigration and emigration)
            "INDUD_KODE".to_string(), // Event type code (indicates immigration or emigration)
        ];

        // Filter by our PNRs of interest
        let valid_pnrs = self.valid_pnrs.clone();
        let pnr_filter_expr = create_pnr_filter_expr(&valid_pnrs);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Add migration data to individuals
        let mut migration_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the PNR
                if let Some(pnr_col) = batch
                    .column_by_name("PNR")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                {
                    if pnr_col.is_null(row) {
                        continue;
                    }

                    let pnr = pnr_col.value(row).to_string();

                    // Set migration dates if we found the individual
                    if let Some(individual) = self.individuals.get_mut(&pnr) {
                        // Extract event date and type
                        if let (Some(date_col), Some(type_col)) = (
                            batch
                                .column_by_name("HAEND_DATO")
                                .and_then(|col| col.as_any().downcast_ref::<StringArray>()),
                            batch
                                .column_by_name("INDUD_KODE")
                                .and_then(|col| col.as_any().downcast_ref::<StringArray>()),
                        ) {
                            if !date_col.is_null(row) && !type_col.is_null(row) {
                                let event_date = date_col.value(row);
                                let event_type = type_col.value(row);

                                // Determine if it's emigration or immigration based on INDUD_KODE
                                // Typical values are "1" for immigration, "0" for emigration
                                // This may need adjustment based on actual coding
                                match event_type {
                                    "0" | "U" | "UDR" => {
                                        // Emigration
                                        if let Ok(date) = chrono::NaiveDate::parse_from_str(
                                            event_date, "%Y-%m-%d",
                                        ) {
                                            individual.emigration_date = Some(date);
                                        }
                                    }
                                    "1" | "I" | "IND" => {
                                        // Immigration
                                        if let Ok(date) = chrono::NaiveDate::parse_from_str(
                                            event_date, "%Y-%m-%d",
                                        ) {
                                            individual.immigration_date = Some(date);
                                        }
                                    }
                                    _ => {
                                        // Unknown code, ignore
                                    }
                                }

                                migration_count += 1;
                            }
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
            "SOCIO13".to_string(), // Socioeconomic status - newer version
            "SOCIO02".to_string(), // Socioeconomic status - older version
            "SOCIO".to_string(),   // Socioeconomic status - basic version
            "SENR".to_string(),    // Workplace ID
        ];

        // For parents, we want to get their employment information
        // Collect all parent PNRs
        let mut parent_pnrs = HashSet::new();
        for (mother, father) in self.relationships.values() {
            if let Some(mother_pnr) = mother {
                parent_pnrs.insert(mother_pnr.clone());
            }
            if let Some(father_pnr) = father {
                parent_pnrs.insert(father_pnr.clone());
            }
        }

        // Combine with our existing PNRs of interest
        let pnrs_of_interest = self
            .valid_pnrs
            .iter()
            .cloned()
            .chain(parent_pnrs.into_iter())
            .collect::<HashSet<_>>();

        let pnr_filter_expr = create_pnr_filter_expr(&pnrs_of_interest);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Add employment data to individuals
        let mut employment_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the PNR
                if let Some(pnr_col) = batch
                    .column_by_name("PNR")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                {
                    if pnr_col.is_null(row) {
                        continue;
                    }

                    let pnr = pnr_col.value(row).to_string();

                    // Now handle parents that aren't in our individuals HashMap
                    if !self.individuals.contains_key(&pnr) {
                        // Create a new minimal individual for this parent
                        let parent = Individual::new(
                            pnr.clone(),
                            crate::models::core::types::Gender::Unknown,
                            None,
                        );

                        // Add to individuals collection
                        self.individuals.insert(pnr.clone(), parent);
                        self.valid_pnrs.insert(pnr.clone());
                        employment_count += 1;
                    }

                    // Update employment data for this individual
                    if let Some(individual) = self.individuals.get_mut(&pnr) {
                        // Try to get socioeconomic status from SOCIO13 (preferred),
                        // then SOCIO02, then SOCIO in that order
                        let mut status_updated = false;

                        // Try SOCIO13 first (newest format)
                        if let Some(socio_col) = batch
                            .column_by_name("SOCIO13")
                            .and_then(|col| col.as_any().downcast_ref::<arrow::array::Int8Array>())
                        {
                            if !socio_col.is_null(row) {
                                let socio_val = socio_col.value(row);
                                individual.socioeconomic_status =
                                    map_socio_to_enum(i32::from(socio_val));
                                status_updated = true;
                            }
                        }

                        // If SOCIO13 failed, try SOCIO02
                        if !status_updated {
                            if let Some(socio_col) =
                                batch.column_by_name("SOCIO02").and_then(|col| {
                                    col.as_any().downcast_ref::<arrow::array::Int8Array>()
                                })
                            {
                                if !socio_col.is_null(row) {
                                    let socio_val = socio_col.value(row);
                                    individual.socioeconomic_status =
                                        map_socio_to_enum(i32::from(socio_val));
                                    status_updated = true;
                                }
                            }
                        }

                        // As a last resort, try SOCIO
                        if !status_updated {
                            if let Some(socio_col) = batch.column_by_name("SOCIO").and_then(|col| {
                                col.as_any().downcast_ref::<arrow::array::Int8Array>()
                            }) {
                                if !socio_col.is_null(row) {
                                    let socio_val = socio_col.value(row);
                                    individual.socioeconomic_status =
                                        map_socio_to_enum(i32::from(socio_val));
                                }
                            }
                        }

                        // Get workplace ID if available
                        if let Some(senr_col) = batch
                            .column_by_name("SENR")
                            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                        {
                            if !senr_col.is_null(row) {
                                individual.workplace_id = Some(senr_col.value(row).to_string());
                            }
                        }

                        employment_count += 1;
                    }
                }
            }
        }

        info!("Added employment information to {employment_count} individuals");

        // Record statistics
        self.stats.insert("akm_enriched".into(), employment_count);

        Ok(())
    }

    /// Step 4b: Load UDDF registry for education information
    async fn load_education_registry(&mut self) -> Result<()> {
        let registry_dir = self.registry_path("UDDF");
        if !registry_dir.exists() || !registry_dir.is_dir() {
            warn!(
                "UDDF registry directory not found: {}",
                registry_dir.display()
            );
            return Ok(());
        }

        info!("Loading UDDF data for education information");

        // UDDF columns
        let columns = vec![
            "PNR".to_string(),     // Person ID
            "HFAUDD".to_string(),  // Highest completed education
            "HF_VFRA".to_string(), // Valid from date
            "HF_VTIL".to_string(), // Valid to date
            "INSTNR".to_string(),  // Institution number
        ];

        // Filter for all individuals of interest
        let pnr_filter_expr = create_pnr_filter_expr(&self.valid_pnrs);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Add education data to individuals
        let mut education_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the PNR
                if let Some(pnr_col) = batch
                    .column_by_name("PNR")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                {
                    if pnr_col.is_null(row) {
                        continue;
                    }

                    let pnr = pnr_col.value(row).to_string();

                    // Set education data if we found the individual
                    if let Some(individual) = self.individuals.get_mut(&pnr) {
                        // Extract education data
                        if let Some(edu_col) = batch
                            .column_by_name("HFAUDD")
                            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                        {
                            if !edu_col.is_null(row) {
                                let edu_code = edu_col.value(row);
                                // Map education code to education level
                                // HFAUDD codes use a 4-digit format where:
                                // 10-30: Low education (ISCED 0-2)
                                // 35-50: Medium education (ISCED 3-4)
                                // 60-80: High education (ISCED 5-8)
                                if let Ok(edu_num) = edu_code.parse::<i32>() {
                                    individual.education_level = match edu_num {
                                        10..=30 => crate::models::core::types::EducationLevel::Low,
                                        35..=50 => {
                                            crate::models::core::types::EducationLevel::Medium
                                        }
                                        60..=80 => crate::models::core::types::EducationLevel::High,
                                        _ => crate::models::core::types::EducationLevel::Unknown,
                                    };
                                }
                            }
                        }

                        // Get institution number if available
                        if let Some(inst_col) = batch
                            .column_by_name("INSTNR")
                            .and_then(|col| col.as_any().downcast_ref::<arrow::array::Int8Array>())
                        {
                            if !inst_col.is_null(row) {
                                let inst_num = inst_col.value(row);
                                individual.education_institution = Some(inst_num.to_string());
                            }
                        }

                        // Get dates if available
                        if let Some(valid_from_col) = batch
                            .column_by_name("HF_VFRA")
                            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                        {
                            if !valid_from_col.is_null(row) {
                                let date_str = valid_from_col.value(row);
                                if let Ok(date) =
                                    chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                                {
                                    individual.education_completion_date = Some(date);
                                }
                            }
                        }

                        education_count += 1;
                    }
                }
            }
        }

        info!("Added education information to {education_count} individuals");

        // Record statistics
        self.stats.insert("uddf_enriched".into(), education_count);

        Ok(())
    }

    /// Step 4c: Load IND registry for income information
    async fn load_income_registry(&mut self) -> Result<()> {
        let registry_dir = self.registry_path("IND");
        if !registry_dir.exists() || !registry_dir.is_dir() {
            warn!(
                "IND registry directory not found: {}",
                registry_dir.display()
            );
            return Ok(());
        }

        info!("Loading IND data for income information");

        // IND columns
        let columns = vec![
            "PNR".to_string(),            // Person ID
            "PERINDKIALT_13".to_string(), // Total personal income
            "LOENMV_13".to_string(),      // Wage income
            "PRE_SOCIO".to_string(),      // Pre-tax socioeconomic classification
            "BESKST13".to_string(),       // Employment status
        ];

        // Filter for all individuals of interest
        let pnr_filter_expr = create_pnr_filter_expr(&self.valid_pnrs);

        let record_batches = load_parquet_files_parallel_with_filter(
            &registry_dir,
            &pnr_filter_expr,
            Some(&columns),
        )?;

        // Add income data to individuals
        let mut income_count = 0;

        for batch in &record_batches {
            for row in 0..batch.num_rows() {
                // Get the PNR
                if let Some(pnr_col) = batch
                    .column_by_name("PNR")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                {
                    if pnr_col.is_null(row) {
                        continue;
                    }

                    let pnr = pnr_col.value(row).to_string();

                    // Set income data if we found the individual
                    if let Some(individual) = self.individuals.get_mut(&pnr) {
                        // Extract total personal income
                        if let Some(income_col) =
                            batch.column_by_name("PERINDKIALT_13").and_then(|col| {
                                col.as_any().downcast_ref::<arrow::array::Float64Array>()
                            })
                        {
                            if !income_col.is_null(row) {
                                let income = income_col.value(row);
                                individual.annual_income = Some(income);
                            }
                        }

                        // Extract employment income
                        if let Some(wage_col) = batch.column_by_name("LOENMV_13").and_then(|col| {
                            col.as_any().downcast_ref::<arrow::array::Float64Array>()
                        }) {
                            if !wage_col.is_null(row) {
                                let wage = wage_col.value(row);
                                individual.employment_income = Some(wage);
                            }
                        }

                        // Extract socioeconomic status if not already set
                        if individual.socioeconomic_status
                            == crate::models::core::types::SocioeconomicStatus::Unknown
                        {
                            if let Some(socio_col) =
                                batch.column_by_name("PRE_SOCIO").and_then(|col| {
                                    col.as_any().downcast_ref::<arrow::array::Int8Array>()
                                })
                            {
                                if !socio_col.is_null(row) {
                                    let socio_val = socio_col.value(row);
                                    individual.socioeconomic_status =
                                        map_socio_to_enum(i32::from(socio_val));
                                }
                            }
                        }

                        // Set income year (hard-coded based on file year, could be extracted from filename)
                        individual.income_year = Some(2013); // Assuming "_13" suffix indicates 2013

                        income_count += 1;
                    }
                }
            }
        }

        info!("Added income information to {income_count} individuals");

        // Record statistics
        self.stats.insert("ind_enriched".into(), income_count);

        Ok(())
    }

    /// Get a vector of all individuals
    #[must_use]
    pub fn get_individuals(&self) -> Vec<Individual> {
        self.individuals.values().cloned().collect()
    }

    /// Get the statistics collected during processing
    #[must_use]
    pub fn get_stats(&self) -> &HashMap<String, usize> {
        &self.stats
    }

    /// Filter a record batch to include only rows with birth dates in the specified range
    fn filter_batch_by_birth_date(
        &self,
        batch: &RecordBatch,
        start_date: &str,
        end_date: &str,
    ) -> Result<Option<RecordBatch>> {
        // Get the birth date column
        let date_col_idx = batch.schema().index_of("FOED_DAG").unwrap_or(1);

        if let Some(date_col) = batch
            .column(date_col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
        {
            // Create indices for rows matching the date range
            let mut indices = Vec::new();

            for i in 0..date_col.len() {
                if !date_col.is_null(i) {
                    let date_str = date_col.value(i);
                    if date_str >= start_date && date_str <= end_date {
                        indices.push(i as u64);
                    }
                }
            }

            // If there are matching rows, create a filtered batch
            if !indices.is_empty() {
                let filtered_batch =
                    take::take_record_batch(batch, &arrow::array::UInt64Array::from(indices))?;

                return Ok(Some(filtered_batch));
            }
        }

        Ok(None)
    }
}

/// Create a PNR filter expression for a set of PNRs
fn create_pnr_filter_expr(pnrs: &HashSet<String>) -> Expr {
    // For simplicity, we'll use an "in list" expression
    // For large PNR sets, you might want to use a different approach
    let pnr_list = pnrs.iter().cloned().collect::<Vec<_>>();
    col("PNR").in_list(pnr_list)
}

/// Run the sequential registry processing example
///
/// This function applies the proper sequence of processing steps:
/// 1. Identify children from BEF based on birth date
/// 2. Get birth details from MFR and match with BEF
/// 3. Add mortality/migration from DOD/VNDS
/// 4. Add socioeconomic info from AKM/UDDF/IND
///
/// # Arguments
/// * `base_dir` - The base directory containing registry subdirectories
///
/// # Returns
/// The number of processed individuals
pub async fn run_sequential_registry_example(
    base_dir: &Path,
    start_date: &str,
    end_date: &str,
) -> Result<usize> {
    // Use the provided date range

    info!("Starting sequential registry processing");
    info!("Base directory: {}", base_dir.display());
    info!("Date range: {start_date} to {end_date}");

    // Create processor
    let mut processor = SequentialRegistryProcessor::new(base_dir, start_date, end_date);

    // Run the processing sequence
    processor.process_all().await?;

    // Get the results
    let individuals = processor.get_individuals();
    let stats = processor.get_stats();
    let total_individuals = individuals.len();

    // Print summary statistics
    println!("Sequential Registry Processing Complete");
    println!("--------------------------------------");
    println!("Date range: {start_date} to {end_date}");
    println!("Total individuals processed: {total_individuals}");
    println!();

    // Print registry-specific statistics
    println!("BEF Registry:");
    println!(
        "  Initial individuals: {}",
        stats.get("bef_individuals").unwrap_or(&0)
    );
    println!();

    println!("Enrichment Statistics:");
    println!(
        "  MFR enriched: {}",
        stats.get("mfr_enriched").unwrap_or(&0)
    );
    println!(
        "  DOD enriched: {}",
        stats.get("dod_enriched").unwrap_or(&0)
    );
    println!(
        "  VNDS enriched: {}",
        stats.get("vnds_enriched").unwrap_or(&0)
    );
    println!(
        "  AKM enriched: {}",
        stats.get("akm_enriched").unwrap_or(&0)
    );
    println!(
        "  UDDF enriched: {}",
        stats.get("uddf_enriched").unwrap_or(&0)
    );
    println!(
        "  IND enriched: {}",
        stats.get("ind_enriched").unwrap_or(&0)
    );
    println!();

    // Print sample individuals if available
    if !individuals.is_empty() {
        let sample_size = std::cmp::min(5, individuals.len());
        println!("Sample Individuals (first {sample_size}):");

        for (i, individual) in individuals.iter().take(sample_size).enumerate() {
            println!(
                "Individual {}: PNR={}, Gender={:?}, Birth date={:?}, Origin={:?}",
                i + 1,
                individual.pnr,
                individual.gender,
                individual.birth_date,
                individual.origin
            );
        }
    }

    Ok(total_individuals)
}
