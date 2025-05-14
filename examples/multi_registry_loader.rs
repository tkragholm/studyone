// use arrow::array::Array;
// use arrow::compute::kernels::take;
// use arrow::record_batch::RecordBatch;
// use log::{info, warn};
// use par_reader::error::Result;
// use par_reader::examples::parrallel_loader::load_parquet_files_parallel_with_filter;
// use par_reader::filter_expression::col;
// use par_reader::models::Individual;
// use par_reader::models::core::individual::serde::SerdeIndividual;
// use par_reader::registry::{akm, bef, deserializer::create_mapped_batch, ind, mfr, uddf, vnds};
// use std::collections::{HashMap, HashSet};
// use std::path::Path;

// /// Supported registry types
// pub enum RegistryType {
//     BEF,
//     AKM,
//     IND,
//     UDDF,
//     MFR,
//     VNDS,
// }

// impl RegistryType {
//     /// Get a string representation of the registry type
//     #[must_use]
//     pub fn as_str(&self) -> &str {
//         match self {
//             Self::BEF => "BEF",
//             Self::AKM => "AKM",
//             Self::IND => "IND",
//             Self::UDDF => "UDDF",
//             Self::MFR => "MFR",
//             Self::VNDS => "VNDS",
//         }
//     }

//     /// Get the standard field mapping for this registry type
//     #[must_use]
//     pub fn field_mapping(&self) -> HashMap<String, String> {
//         match self {
//             Self::BEF => bef::deserializer::field_mapping(),
//             Self::AKM => akm::deserializer::field_mapping(),
//             Self::IND => ind::deserializer::field_mapping(),
//             Self::UDDF => uddf::deserializer::field_mapping(),
//             Self::MFR => mfr::deserializer::field_mapping(),
//             Self::VNDS => vnds::deserializer::field_mapping(),
//         }
//     }

//     /// Get common column names that should be present in all registry types
//     #[must_use]
//     pub fn common_columns() -> Vec<String> {
//         vec![
//             "PNR".to_string(),      // Person identifier (common to all)
//             "FOED_DAG".to_string(), // Birth date (primary filter field)
//             "KOEN".to_string(),     // Gender
//             "CIVST".to_string(),    // Marital status
//         ]
//     }

//     /// Get registry-specific columns to include
//     #[must_use]
//     pub fn specific_columns(&self) -> Vec<String> {
//         match self {
//             Self::BEF => vec![
//                 "FAR_ID".to_string(),
//                 "MOR_ID".to_string(),
//                 "FAMILIE_ID".to_string(),
//                 "STATSB".to_string(),
//             ],
//             Self::AKM => vec![
//                 "ARB_STED_ID".to_string(),
//                 "BRANCHE".to_string(),
//                 "DISCO".to_string(),
//                 "SOCIO".to_string(),
//             ],
//             Self::IND => vec![
//                 "PERINDKIALT".to_string(),
//                 "DISPON_NY".to_string(),
//                 "LOENMV".to_string(),
//                 "NETOVSKUD".to_string(),
//             ],
//             Self::MFR => vec![
//                 "GRAVIDITET".to_string(),
//                 "FLERFOLD".to_string(),
//                 "APGAR".to_string(),
//                 "VAEGT".to_string(),
//             ],
//             Self::UDDF | Self::VNDS => vec![
//                 // These registries have fewer standard fields
//                 // Include any registry-specific fields here
//             ],
//         }
//     }
// }

// /// Registry data collection from multiple sources
// pub struct RegistryCollection {
//     /// Individuals loaded from all registry types
//     pub individuals: Vec<Individual>,

//     /// Statistics about the loaded data
//     pub stats: HashMap<String, usize>,
// }

// /// Load data from multiple registry types
// ///
// /// This function loads data from multiple registry types, applying the same
// /// date-based filter to all of them, and converts the result into Individual models.
// ///
// /// # Arguments
// /// * `base_dir` - Base directory containing registry subdirectories
// /// * `registry_types` - Which registry types to load
// /// * `date_start` - Start date for filtering (inclusive)
// /// * `date_end` - End date for filtering (inclusive)
// ///
// /// # Returns
// /// A collection of individuals and statistics about the loaded data
// pub async fn load_multi_registry_data(
//     base_dir: &Path,
//     registry_types: &[RegistryType],
//     date_start: &str,
//     date_end: &str,
// ) -> Result<RegistryCollection> {
//     let mut all_individuals = Vec::new();
//     let mut stats = HashMap::new();

//     // For each registry type, load the data and convert to individuals
//     for registry_type in registry_types {
//         let registry_name = registry_type.as_str();
//         let registry_dir = base_dir.join(registry_name.to_lowercase());

//         if !registry_dir.exists() || !registry_dir.is_dir() {
//             warn!("Registry directory not found: {}", registry_dir.display());
//             stats.insert(format!("{registry_name}_skipped"), 1);
//             continue;
//         }

//         info!(
//             "Loading {} data from {}",
//             registry_name,
//             registry_dir.display()
//         );

//         // Combine common columns with registry-specific ones
//         let mut columns_vec = RegistryType::common_columns();
//         columns_vec.extend(registry_type.specific_columns());

//         // Use a non-filtering approach first to get all data
//         let date_filter = col("FOED_DAG").contains(""); // This will match any non-null date

//         // Load the registry data
//         let record_batches = match load_parquet_files_parallel_with_filter(
//             &registry_dir,
//             &date_filter,
//             Some(&columns_vec[..]),
//         ) {
//             Ok(batches) => {
//                 info!(
//                     "Loaded {} record batches from {}",
//                     batches.len(),
//                     registry_name
//                 );
//                 stats.insert(format!("{registry_name}_batches"), batches.len());

//                 let total_rows = batches
//                     .iter()
//                     .map(arrow::array::RecordBatch::num_rows)
//                     .sum::<usize>();

//                 stats.insert(format!("{registry_name}_total_rows"), total_rows);
//                 info!("Total rows in {registry_name}: {total_rows}");

//                 batches
//             }
//             Err(e) => {
//                 warn!("Failed to load {registry_name} data: {e}");
//                 stats.insert(format!("{registry_name}_error"), 1);
//                 continue;
//             }
//         };

//         // Filter by date range and convert to Individual models
//         info!("Filtering {registry_name} data by date range: {date_start} to {date_end}");
//         let (individuals, filtered_stats) =
//             filter_and_convert_registry(record_batches, registry_type, date_start, date_end)?;

//         // Add the filtered individuals to our collection
//         let count = individuals.len();
//         all_individuals.extend(individuals);

//         // Add statistics
//         for (key, value) in filtered_stats {
//             stats.insert(format!("{registry_name}_{key}"), value);
//         }

//         info!("Added {count} individuals from {registry_name}");
//     }

//     // Add overall statistics
//     stats.insert("total_individuals".to_string(), all_individuals.len());

//     Ok(RegistryCollection {
//         individuals: all_individuals,
//         stats,
//     })
// }

// /// Filter record batches by date range and convert to Individual models
// ///
// /// This function applies date filtering to record batches and converts them
// /// to Individual domain models using the appropriate deserializer.
// ///
// /// # Arguments
// /// * `record_batches` - The Arrow record batches to filter and convert
// /// * `registry_type` - The type of registry data
// /// * `date_start` - Start date for filtering (inclusive)
// /// * `date_end` - End date for filtering (inclusive)
// ///
// /// # Returns
// /// A tuple of (individuals, statistics) where statistics is a `HashMap`
// fn filter_and_convert_registry(
//     record_batches: Vec<RecordBatch>,
//     registry_type: &RegistryType,
//     date_start: &str,
//     date_end: &str,
// ) -> Result<(Vec<Individual>, HashMap<String, usize>)> {
//     let mut filtered_batches: Vec<RecordBatch> = Vec::new();
//     let mut individuals: Vec<Individual> = Vec::new();
//     let mut filtered_rows = 0;
//     let mut stats = HashMap::new();

//     // Process record batches to filter by date range
//     for batch in &record_batches {
//         // 1. Filter the batch by date range
//         let date_col_idx = batch.schema().index_of("FOED_DAG").unwrap_or(1);

//         if let Some(date_col) = batch
//             .column(date_col_idx)
//             .as_any()
//             .downcast_ref::<arrow::array::StringArray>()
//         {
//             // Create a filtered version of this batch with matching dates
//             let mut indices = Vec::new();

//             for i in 0..date_col.len() {
//                 if !date_col.is_null(i) {
//                     let date_str = date_col.value(i);
//                     if date_str >= date_start && date_str <= date_end {
//                         indices.push(i as u64);
//                         filtered_rows += 1;
//                     }
//                 }
//             }

//             // If there are matching rows, create a filtered batch
//             if !indices.is_empty() {
//                 // Create a new RecordBatch with just the selected rows
//                 let filtered_batch =
//                     take::take_record_batch(batch, &arrow::array::UInt64Array::from(indices))?;

//                 filtered_batches.push(filtered_batch);
//             }
//         }
//     }

//     stats.insert("filtered_row_count".to_string(), filtered_rows);
//     stats.insert("filtered_batch_count".to_string(), filtered_batches.len());

//     // Keep track of error types to avoid printing the same error repeatedly
//     let mut seen_errors = HashSet::new();
//     let mut error_count = 0;

//     // 2. Convert filtered batches to Individual models
//     for (batch_idx, batch) in filtered_batches.iter().enumerate() {
//         // Get the registry-specific field mapping
//         let field_mapping = registry_type.field_mapping();

//         // Apply field mapping to match the expected field names
//         let mapped_batch = match create_mapped_batch(batch, field_mapping) {
//             Ok(batch) => batch,
//             Err(e) => {
//                 let error_msg = e.to_string();
//                 if seen_errors.insert(error_msg.clone()) {
//                     // Only log unique errors
//                     warn!(
//                         "[{}] Failed to map batch {}: {}",
//                         registry_type.as_str(),
//                         batch_idx,
//                         error_msg
//                     );
//                 }
//                 error_count += 1;
//                 continue;
//             }
//         };

//         // Use SerdeIndividual to deserialize the batch
//         match SerdeIndividual::from_batch(&mapped_batch) {
//             Ok(serde_individuals) => {
//                 // Convert SerdeIndividual to regular Individual
//                 let batch_individuals = serde_individuals
//                     .into_iter()
//                     .map(SerdeIndividual::into_inner)
//                     .collect::<Vec<_>>();

//                 // Add to our collection
//                 individuals.extend(batch_individuals);
//             }
//             Err(e) => {
//                 let error_msg = e.to_string();
//                 if seen_errors.insert(error_msg.clone()) {
//                     // Only log unique errors
//                     warn!(
//                         "[{}] Failed to deserialize batch {}: {}",
//                         registry_type.as_str(),
//                         batch_idx,
//                         error_msg
//                     );
//                 }
//                 error_count += 1;
//             }
//         }
//     }

//     stats.insert("error_count".to_string(), error_count);
//     stats.insert("error_types".to_string(), seen_errors.len());
//     stats.insert("success_count".to_string(), individuals.len());

//     // If we had errors, print a summary
//     if error_count > 0 {
//         warn!(
//             "[{}] {} batches failed to deserialize with {} unique error types",
//             registry_type.as_str(),
//             error_count,
//             seen_errors.len()
//         );

//         // Print a few examples of error types if there are multiple
//         if seen_errors.len() > 1 {
//             warn!("[{}] Error types include:", registry_type.as_str());
//             for (i, error) in seen_errors.iter().take(3).enumerate() {
//                 warn!("[{}]   {}. {}", registry_type.as_str(), i + 1, error);
//             }
//             if seen_errors.len() > 3 {
//                 warn!(
//                     "[{}]   ... and {} more error types",
//                     registry_type.as_str(),
//                     seen_errors.len() - 3
//                 );
//             }
//         }
//     }

//     Ok((individuals, stats))
// }

// /// Run example loading and filtering multiple registry types
// ///
// /// This function serves as the main entry point for the multi-registry example.
// /// It loads data from different registry types, filters by date range, and
// /// converts to Individual models.
// ///
// /// # Arguments
// /// * `base_dir` - Base directory containing registry subdirectories
// ///
// /// # Returns
// /// The number of individuals loaded from all registries
// pub async fn run_multi_registry_example(base_dir: &Path) -> Result<usize> {
//     // The date range to filter by
//     let date_start = "1995-01-01";
//     let date_end = "2018-12-31";

//     // Registry types to load
//     let registry_types = vec![
//         RegistryType::BEF,
//         RegistryType::AKM,
//         RegistryType::IND,
//         RegistryType::UDDF,
//         RegistryType::MFR,
//         RegistryType::VNDS,
//     ];

//     // Load data from all registry types
//     info!(
//         "Loading data from multiple registry types at {}",
//         base_dir.display()
//     );
//     info!("Filter: birth dates between {date_start} and {date_end}");

//     let collection =
//         load_multi_registry_data(base_dir, &registry_types, date_start, date_end).await?;

//     // Print summary statistics
//     let total_individuals = collection
//         .stats
//         .get("total_individuals")
//         .copied()
//         .unwrap_or(0);

//     println!("Summary of loaded registry data:");
//     println!("--------------------------------");
//     println!("Date range: {date_start} to {date_end}");
//     println!("Total individuals: {total_individuals}");
//     println!();

//     // Print registry-specific statistics
//     for registry_type in &registry_types {
//         let registry_name = registry_type.as_str();
//         let success_count = collection
//             .stats
//             .get(&format!("{registry_name}_success_count"))
//             .copied()
//             .unwrap_or(0);
//         let total_rows = collection
//             .stats
//             .get(&format!("{registry_name}_total_rows"))
//             .copied()
//             .unwrap_or(0);

//         if total_rows > 0 {
//             println!("{registry_name} Registry:");
//             println!("  Total rows: {total_rows}");
//             println!("  Individuals in date range: {success_count}");
//             println!(
//                 "  Filtering rate: {:.2}%",
//                 (success_count as f64 / total_rows as f64) * 100.0
//             );
//         } else if collection
//             .stats
//             .get(&format!("{registry_name}_skipped"))
//             .is_some()
//         {
//             println!("{registry_name} Registry: Directory not found, skipped");
//         } else if collection
//             .stats
//             .get(&format!("{registry_name}_error"))
//             .is_some()
//         {
//             println!("{registry_name} Registry: Error loading data");
//         }
//     }
//     println!();

//     // Print sample individuals if available
//     if !collection.individuals.is_empty() {
//         let sample_size = std::cmp::min(5, collection.individuals.len());
//         println!("Sample Individuals (first {sample_size}):");

//         for (i, individual) in collection.individuals.iter().take(sample_size).enumerate() {
//             println!(
//                 "Individual {}: PNR={}, Gender={:?}, Birth date={:?}, Origin={:?}",
//                 i + 1,
//                 individual.pnr,
//                 individual.gender,
//                 individual.birth_date,
//                 individual.origin
//             );
//         }
//     }

//     Ok(total_individuals)
// }

fn main() {
    print!("Example");
}
