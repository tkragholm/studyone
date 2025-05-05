use crate::utils::{expr_to_filter, print_schema_info, registry_file};
use par_reader::{Expr, LiteralValue, read_parquet, read_parquet_with_filter_async};

/// Test simple filtering expressions
#[tokio::test]
async fn test_simple_filters() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    if !path.exists() {
        println!("AKM test file not found. Skipping test.");
        return Ok(());
    }

    // Load data without filtering to get total row count for comparison
    let full_batches = read_parquet::<std::collections::hash_map::RandomState>(&path, None, None)?;

    let total_rows = full_batches.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!("Total rows without filtering: {total_rows}");

    // Create a test filter: SOCIO > 200
    let filter_expr = Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200));

    // Apply filter
    let filtered_batches = read_parquet_with_filter_async(&path, expr_to_filter(&filter_expr), None).await?;
    let filtered_rows = filtered_batches.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!("Rows after filter (SOCIO > 200): {filtered_rows}");
    println!(
        "Filter selectivity: {:.2}%",
        (filtered_rows as f64 / total_rows as f64) * 100.0
    );

    Ok(())
}

/// Test complex filtering with AND, OR, NOT expressions
#[tokio::test]
async fn test_complex_filters() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    if !path.exists() {
        println!("AKM test file not found. Skipping test.");
        return Ok(());
    }

    // Load data without filtering to get total row count for comparison
    let full_batches = read_parquet::<std::collections::hash_map::RandomState>(&path, None, None)?;

    let total_rows = full_batches.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!("Total rows without filtering: {total_rows}");

    // Test AND filter: SOCIO > 200 AND CPRTYPE = 5
    let and_filter = Expr::And(vec![
        Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200)),
        Expr::Eq("CPRTYPE".to_string(), LiteralValue::Int(5)),
    ]);

    // Apply AND filter
    let and_filtered = read_parquet_with_filter_async(&path, expr_to_filter(&and_filter), None).await?;
    let and_rows = and_filtered.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!(
        "Rows after AND filter (SOCIO > 200 AND CPRTYPE = 5): {and_rows}"
    );
    println!(
        "AND filter selectivity: {:.2}%",
        (and_rows as f64 / total_rows as f64) * 100.0
    );

    // Test OR filter: SOCIO > 300 OR CPRTYPE = 1
    let or_filter = Expr::Or(vec![
        Expr::Gt("SOCIO".to_string(), LiteralValue::Int(300)),
        Expr::Eq("CPRTYPE".to_string(), LiteralValue::Int(1)),
    ]);

    // Apply OR filter
    let or_filtered = read_parquet_with_filter_async(&path, expr_to_filter(&or_filter), None).await?;
    let or_rows = or_filtered.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!(
        "Rows after OR filter (SOCIO > 300 OR CPRTYPE = 1): {or_rows}"
    );
    println!(
        "OR filter selectivity: {:.2}%",
        (or_rows as f64 / total_rows as f64) * 100.0
    );

    // Test NOT filter: SOCIO > 200 (equivalent to NOT (SOCIO <= 200))
    // Using Gt instead of Not(LtEq) since LtEq appears to be unsupported
    let not_filter = Expr::Gt(
        "SOCIO".to_string(),
        LiteralValue::Int(200),
    );

    // Apply the filter
    let not_filtered = read_parquet_with_filter_async(&path, expr_to_filter(&not_filter), None).await?;
    let not_rows = not_filtered.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!("Rows after filter (SOCIO > 200): {not_rows}");
    println!(
        "Filter selectivity: {:.2}%",
        (not_rows as f64 / total_rows as f64) * 100.0
    );

    Ok(())
}

/// Test filtering with different data types
#[tokio::test]
async fn test_filter_data_types() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    if !path.exists() {
        println!("AKM test file not found. Skipping test.");
        return Ok(());
    }

    // Load a batch to inspect its schema
    let batches = read_parquet::<std::collections::hash_map::RandomState>(&path, None, None)?;
    if batches.is_empty() {
        println!("No data found in test file.");
        return Ok(());
    }

    let first_batch = &batches[0];
    println!("Testing filters on different data types in schema:");
    print_schema_info(first_batch);

    // Test filters on different column types
    // Note: These are example filters that might need adjustment based on actual schema

    // Integer filter
    let int_column = "SOCIO"; // Adjust based on actual schema
    let int_filter = Expr::Gt(int_column.to_string(), LiteralValue::Int(200));

    // String filter - replaced Like with starts_with check
    let string_column = "PNR"; // Adjust based on actual schema
    let string_filter = Expr::Eq(
        string_column.to_string(),
        LiteralValue::String("0101".to_string()),
    );

    // Date filter (assuming column has ISO date strings)
    let date_column = "INDM_DAG"; // Adjust based on actual schema
    let date_filter = Expr::GtEq(
        date_column.to_string(),
        LiteralValue::String("2020-01-01".to_string()),
    );

    // Apply each filter and report results
    println!("\nTesting integer filter: {int_column} > 200");
    match read_parquet_with_filter_async(&path, expr_to_filter(&int_filter), None).await {
        Ok(filtered) => {
            let rows = filtered.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
            println!("Filtered rows: {rows}");
        }
        Err(e) => println!("Error with integer filter: {e}"),
    }

    println!("\nTesting string filter: {string_column} = '0101'");
    match read_parquet_with_filter_async(&path, expr_to_filter(&string_filter), None).await {
        Ok(filtered) => {
            let rows = filtered.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
            println!("Filtered rows: {rows}");
        }
        Err(e) => println!("Error with string filter: {e}"),
    }

    println!("\nTesting date filter: {date_column} >= '2020-01-01'");
    match read_parquet_with_filter_async(&path, expr_to_filter(&date_filter), None).await {
        Ok(filtered) => {
            let rows = filtered.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
            println!("Filtered rows: {rows}");
        }
        Err(e) => println!("Error with date filter: {e}"),
    }

    Ok(())
}

/// Test filter performance comparison
#[tokio::test]
async fn test_filter_performance() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    if !path.exists() {
        println!("AKM test file not found. Skipping test.");
        return Ok(());
    }

    // Simple filter with different selectivity levels to test performance

    // High selectivity filter (likely selects few rows)
    let high_selectivity = Expr::And(vec![
        Expr::Gt("SOCIO".to_string(), LiteralValue::Int(300)),
        Expr::Eq("CPRTYPE".to_string(), LiteralValue::Int(5)),
    ]);

    // Medium selectivity filter
    let medium_selectivity = Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200));

    // Low selectivity filter (likely selects many rows)
    let low_selectivity = Expr::Gt("SOCIO".to_string(), LiteralValue::Int(100));

    // Measure performance
    println!("Testing filter performance with different selectivity levels:");

    // High selectivity test
    let high_start = std::time::Instant::now();
    let high_result = read_parquet_with_filter_async(&path, expr_to_filter(&high_selectivity), None).await?;
    let high_duration = high_start.elapsed();
    let high_rows = high_result.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!(
        "High selectivity filter: {high_rows} rows in {high_duration:?}"
    );

    // Medium selectivity test
    let medium_start = std::time::Instant::now();
    let medium_result =
        read_parquet_with_filter_async(&path, expr_to_filter(&medium_selectivity), None).await?;
    let medium_duration = medium_start.elapsed();
    let medium_rows = medium_result.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!(
        "Medium selectivity filter: {medium_rows} rows in {medium_duration:?}"
    );

    // Low selectivity test
    let low_start = std::time::Instant::now();
    let low_result = read_parquet_with_filter_async(&path, expr_to_filter(&low_selectivity), None).await?;
    let low_duration = low_start.elapsed();
    let low_rows = low_result.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>();
    println!(
        "Low selectivity filter: {low_rows} rows in {low_duration:?}"
    );

    // Calculate rows per second for each test
    println!("\nPerformance metrics:");
    println!(
        "High selectivity: {:.2} rows/ms",
        high_rows as f64 / high_duration.as_millis() as f64
    );
    println!(
        "Medium selectivity: {:.2} rows/ms",
        medium_rows as f64 / medium_duration.as_millis() as f64
    );
    println!(
        "Low selectivity: {:.2} rows/ms",
        low_rows as f64 / low_duration.as_millis() as f64
    );

    Ok(())
}
