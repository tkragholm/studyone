#[cfg(test)]
mod tests {
    use crate::utils::test_data_dir;
    use par_reader::registry::RegisterLoader;
    use par_reader::registry::bef::BefRegister;
    use std::error::Error;

    #[tokio::test]
    async fn test_bef_basic_loading() -> Result<(), Box<dyn Error>> {
        let register = BefRegister::new();
        let test_path = test_data_dir().join("bef");
        
        // Check if test data exists
        if !test_path.exists() {
            println!("Test data not found at {}, skipping test", test_path.display());
            return Ok(());
        }

        let result = register.load_async(&test_path, None).await?;

        println!("Loaded {} batches from BEF register", result.len());
        println!(
            "Total rows: {}",
            result.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>()
        );

        Ok(())
    }
}
