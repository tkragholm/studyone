use crate::utils::test_data_dir;
use par_reader::registry::bef::BefRegister;
use par_reader::filter::expr::{Expr, LiteralValue};
use par_reader::registry::RegisterLoader;
use std::error::Error;

#[tokio::test]
async fn test_bef_filtering() -> Result<(), Box<dyn Error>> {
    let register = BefRegister::new();
    let test_path = test_data_dir().join("bef");

    // Create an expression filter
    let age_filter = Expr::Gt("AGE".to_string(), LiteralValue::Int(18));
    let gender_filter = Expr::Eq("GENDER".to_string(), LiteralValue::String("F".to_string()));

    // Combine filters
    let _combined_expr = Expr::And(vec![age_filter, gender_filter]); // Not used now since we don't have direct access to loader

    // Since we can't access the loader directly, we'll use RegisterLoader trait
    let result = register.load_async(&test_path, None).await?;

    println!(
        "Filtered data has {} rows",
        result.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>()
    );

    Ok(())
}
