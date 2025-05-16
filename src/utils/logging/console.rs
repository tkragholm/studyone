//! Console output utilities
//!
//! This module provides utilities for formatted console output.

use arrow::record_batch::RecordBatch;

/// Print summary information about record batches
pub fn print_batch_summary(batches: &[RecordBatch], elapsed: std::time::Duration) {
    println!("Read {} record batches in {:?}", batches.len(), elapsed);
    println!(
        "Total rows: {}",
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
}

/// Print detailed schema information from the first batch
pub fn print_schema_info(batch: &RecordBatch) {
    println!("Schema:");
    for field in batch.schema().fields() {
        println!("  - {} ({})", field.name(), field.data_type());
    }
}

/// Print sample rows from a batch
pub fn print_sample_rows(batch: &RecordBatch, num_rows: usize) {
    println!("First {num_rows} rows:");
    for row_idx in 0..std::cmp::min(num_rows, batch.num_rows()) {
        print!("Row {row_idx}: [");
        for col_idx in 0..batch.num_columns() {
            let column = batch.column(col_idx);
            print!("{}: ", batch.schema().field(col_idx).name());

            if column.is_null(row_idx) {
                print!("NULL");
            } else {
                print!("Value"); // Simplified - actual value display would depend on column type
            }

            if col_idx < batch.num_columns() - 1 {
                print!(", ");
            }
        }
        println!("]");
    }
}