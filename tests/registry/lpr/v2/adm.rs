use crate::utils::test_data_dir;
use par_reader::registry::RegisterLoader;
use par_reader::registry::lpr::v2::LprAdmRegister;
use std::error::Error;

#[tokio::test]
async fn test_lpr_adm_loading() -> Result<(), Box<dyn Error>> {
    let register = LprAdmRegister::new();
    let test_path = test_data_dir().join("lpr_adm");

    // Check if test data exists
    if !test_path.exists() {
        println!(
            "Test data not found at {}, skipping test",
            test_path.display()
        );
        return Ok(());
    }

    let result = register.load_async(&test_path, None).await?;

    println!("Loaded {} batches from LPR_ADM register", result.len());
    println!(
        "Total rows: {}",
        result.iter().map(par_reader::RecordBatch::num_rows).sum::<usize>()
    );

    Ok(())
}
