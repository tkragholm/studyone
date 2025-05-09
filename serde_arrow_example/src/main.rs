use arrow::datatypes::{FieldRef, Schema};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};

// Define an enum for Gender similar to your codebase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Gender {
    Unknown = 0,
    Male = 1,
    Female = 2,
}

impl From<i32> for Gender {
    fn from(value: i32) -> Self {
        match value {
            1 => Gender::Male,
            2 => Gender::Female,
            _ => Gender::Unknown,
        }
    }
}

// Define a struct similar to Individual but with fewer fields for simplicity
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Individual {
    pnr: String,
    gender: Gender,
    birth_date: Option<NaiveDate>,
    municipality_code: Option<String>,
    is_rural: bool,
    mother_pnr: Option<String>,
    father_pnr: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create sample data
    let individuals = vec![
        Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(1980, 1, 1).unwrap()),
            municipality_code: Some("0101".to_string()),
            is_rural: false,
            mother_pnr: Some("0987654321".to_string()),
            father_pnr: None,
        },
        Individual {
            pnr: "0987654321".to_string(),
            gender: Gender::Female,
            birth_date: Some(NaiveDate::from_ymd_opt(1950, 5, 10).unwrap()),
            municipality_code: Some("0202".to_string()),
            is_rural: true,
            mother_pnr: None,
            father_pnr: None,
        },
    ];

    println!("Original data:");
    for (i, individual) in individuals.iter().enumerate() {
        println!("Individual {}: {:#?}", i, individual);
    }

    // Get Arrow schema from samples instead of from type
    println!("\nGenerating schema from Individual samples...");
    let fields = Vec::<FieldRef>::from_samples(
        &individuals,
        TracingOptions::default().allow_null_fields(true),
    )?;
    let schema = Schema::new(fields.iter().cloned().collect::<Vec<_>>());
    println!("Generated schema: {:#?}", schema);

    // Convert Vec<Individual> to RecordBatch
    println!("\nConverting Vec<Individual> to RecordBatch...");
    let batch = serde_arrow::to_record_batch(&fields, &individuals)?;
    println!("RecordBatch created with {} rows", batch.num_rows());
    println!("RecordBatch schema: {:#?}", batch.schema());

    // Convert RecordBatch back to Vec<Individual>
    println!("\nConverting RecordBatch back to Vec<Individual>...");
    let individuals_from_batch: Vec<Individual> = match serde_arrow::from_record_batch(&batch) {
        Ok(result) => result,
        Err(e) => {
            println!("Error during deserialization: {:?}", e);
            return Err(Box::new(e));
        }
    };
    println!(
        "Deserialized {} individuals from RecordBatch",
        individuals_from_batch.len()
    );

    // Display the deserialized individuals
    for (i, individual) in individuals_from_batch.iter().enumerate() {
        println!("Individual {}: {:#?}", i, individual);
    }

    // Verify the data is identical
    assert_eq!(individuals.len(), individuals_from_batch.len());
    for (original, deserialized) in individuals.iter().zip(individuals_from_batch.iter()) {
        assert_eq!(original, deserialized);
    }

    println!("\nVerification complete - data matches!");

    Ok(())
}
