#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{Expr, FilterBuilder};
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_ind_with_year_filter() -> Result<()> {
        let register = IndRegister::for_year(2018);
        let test_path = PathBuf::from("test_data/ind");

        let result = register.load_async(&test_path, None).await?;

        println!("Loaded {} batches for year 2018", result.len());
        println!(
            "Total rows: {}",
            result.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ind_with_complex_filters() -> Result<()> {
        let register = IndRegister::new();
        let test_path = PathBuf::from("test_data/ind");

        // Create a complex filter: income > 500000 AND age < 65
        let income_filter = Expr::Gt("PERINDKIALT".to_string(), 500000.into());
        let age_filter = Expr::Lt("ALDER".to_string(), 65.into());

        // Combine filters
        let combined_expr = FilterBuilder::from_expr(income_filter)
            .and_expr(age_filter)
            .build();

        // Convert to a batch filter
        let filter = Arc::new(crate::filter::expr::ExpressionFilter::new(combined_expr));

        // Use the filter with the loader
        let filtered_data = register
            .loader
            .load_with_filter_async(&test_path, filter)
            .await?;

        println!(
            "Filtered income data has {} rows",
            filtered_data.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok(())
    }
}
