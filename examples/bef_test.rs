//! Test for the Registry trait with BEF data
//!
//! This is a test for the new macro-based BEF registry implementation

use par_reader::registry::RegisterLoader;
use par_reader::registry::bef::{create_deserializer, deserialize_batch};

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing BEF Registry with macro-based implementation");

    // Create the deserializer
    let deserializer = create_deserializer();
    println!(
        "Created deserializer for {} registry",
        deserializer.get_register_name()
    );

    // Test loading data from the specified path
    let base_path = "/Users/tobiaskragholm/generated_data/parquet/bef";
    println!("Loading and analyzing data from directory: {base_path}");

    // First list the files in the directory to find Parquet files
    use std::collections::HashMap;
    use std::fs;

    // Statistics tracking
    let mut total_individuals = 0;
    let mut total_properties = 0;
    let mut field_stats: HashMap<String, usize> = HashMap::new();

    let entries = match fs::read_dir(base_path) {
        Ok(entries) => entries,
        Err(e) => {
            println!("Error reading directory: {e}");
            return Ok(());
        }
    };

    // Look for Parquet files
    let mut parquet_files = Vec::new();
    for entry in entries {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                parquet_files.push(path);
            }
        }
    }

    // Function to process individuals and gather statistics
    let mut process_individuals = |individuals: Vec<par_reader::models::core::Individual>| {
        total_individuals += individuals.len();

        // Process each individual to gather field statistics
        for individual in &individuals {
            if let Some(props) = individual.properties() {
                total_properties += props.len();

                // Count occurrences of each property
                for key in props.keys() {
                    *field_stats.entry(key.clone()).or_insert(0) += 1;
                }
            }
        }
    };

    // Process files or directory
    if parquet_files.is_empty() {
        // Try using the directory path directly in case the RegisterLoader can handle directories
        let path = Path::new(base_path);

        match deserializer.load(path, None) {
            Ok(batches) => {
                println!("Found {} batches in directory", batches.len());

                for (i, batch) in batches.iter().enumerate() {
                    match deserialize_batch(&deserializer, batch) {
                        Ok(individuals) => {
                            process_individuals(individuals);
                        }
                        Err(e) => {
                            println!("Error deserializing batch {i}: {e}");
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error loading from directory: {e}");
            }
        }
    } else {
        // Process each Parquet file
        println!("Found {} Parquet files", parquet_files.len());

        for parquet_file in &parquet_files {
            match deserializer.load(parquet_file, None) {
                Ok(batches) => {
                    for (i, batch) in batches.iter().enumerate() {
                        match deserialize_batch(&deserializer, batch) {
                            Ok(individuals) => {
                                process_individuals(individuals);
                            }
                            Err(e) => {
                                println!(
                                    "Error deserializing batch {i} from file {parquet_file:?}: {e}"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Error loading from file {parquet_file:?}: {e}");
                }
            }
        }
    }

    // Print statistics
    println!("\n--- MODEL STATISTICS ---");
    println!("Total individuals loaded: {total_individuals}");
    println!("Total properties: {total_properties}");
    println!(
        "Average properties per individual: {:.2}",
        if total_individuals > 0 {
            total_properties as f64 / total_individuals as f64
        } else {
            0.0
        }
    );

    // Sort fields by frequency and print top fields
    let mut field_counts: Vec<(String, usize)> = field_stats.into_iter().collect();
    field_counts.sort_by(|a, b| b.1.cmp(&a.1));

    println!("\nField population statistics:");
    println!(
        "{:<20} | {:<10} | {:<10}",
        "Field Name", "Count", "Percentage"
    );
    println!("{:-<20}-|-{:-<10}-|-{:-<10}", "", "", "");

    for (field, count) in field_counts {
        let percentage = if total_individuals > 0 {
            (count as f64 / total_individuals as f64) * 100.0
        } else {
            0.0
        };

        println!("{field:<20} | {count:<10} | {percentage:.2}%");
    }

    // We've already processed the batches in the previous section
    println!("BEF Registry test completed successfully!");
    Ok(())
}
