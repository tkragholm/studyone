//! Schema-aware constructors for Income models
//!
//! This module extends the Income model with schema-specific constructors
//! that understand how to build Income objects directly from registry schemas.

use crate::error::Result;
use crate::models::income::Income;
use crate::utils::array_utils::{downcast_array, get_column};
use arrow::array::{Array, Float64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// Income type identifiers matching the previous adapter implementation
pub enum IncomeType {
    /// Total personal income
    TotalPersonal,
    /// Salary income
    Salary,
    /// Self-employment income
    SelfEmployment,
    /// Capital income
    Capital,
    /// Transfer payments
    TransferPayments,
    /// Other income
    Other,
}

impl IncomeType {
    /// Convert income type to string representation
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::TotalPersonal => "total_personal",
            Self::Salary => "salary",
            Self::SelfEmployment => "self_employment",
            Self::Capital => "capital",
            Self::TransferPayments => "transfer_payments",
            Self::Other => "other",
        }
    }
}

impl Income {
    /// Create a new Income record from an IND registry record
    /// 
    /// # Arguments
    /// * `batch` - Record batch containing IND registry data
    /// * `row` - Row index in the batch
    /// * `year` - Year of the income
    /// * `income_type` - Type of income to extract
    /// * `adjust_inflation` - Optional function to adjust for inflation
    ///
    /// # Returns
    /// * `Result<Option<Income>>` - Income record if the data is available
    pub fn from_ind_record(
        batch: &RecordBatch,
        row: usize,
        year: i32,
        income_type: &IncomeType,
        adjust_inflation: Option<&dyn Fn(f64) -> f64>,
    ) -> Result<Option<Income>> {
        // Extract PNR column
        let pnr_array_opt = get_column(batch, "PNR", &DataType::Utf8, true)?;
        let pnr_array = match &pnr_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "PNR", "String")?,
            None => return Ok(None), // Required column missing
        };

        // Extract the appropriate income column based on income type
        let (_column_name, income_value) = match income_type {
            IncomeType::TotalPersonal => {
                let array_opt = get_column(batch, "PERINDKIALT_13", &DataType::Float64, false)?;
                match array_opt {
                    Some(array) => {
                        let float_array = downcast_array::<Float64Array>(&array, "PERINDKIALT_13", "Float64")?;
                        if row < float_array.len() && !float_array.is_null(row) {
                            ("PERINDKIALT_13", float_array.value(row))
                        } else {
                            return Ok(None); // No total income data
                        }
                    }
                    None => return Ok(None), // Column not found
                }
            }
            IncomeType::Salary => {
                let array_opt = get_column(batch, "LOENMV_13", &DataType::Float64, false)?;
                match array_opt {
                    Some(array) => {
                        let float_array = downcast_array::<Float64Array>(&array, "LOENMV_13", "Float64")?;
                        if row < float_array.len() && !float_array.is_null(row) {
                            ("LOENMV_13", float_array.value(row))
                        } else {
                            return Ok(None); // No salary data
                        }
                    }
                    None => return Ok(None), // Column not found
                }
            }
            // Other income types would require additional columns or derivation
            _ => return Ok(None),
        };

        // Apply inflation adjustment if provided
        let adjusted_amount = match adjust_inflation {
            Some(adjust_fn) => adjust_fn(income_value),
            None => income_value,
        };

        // Get PNR
        if row >= pnr_array.len() || pnr_array.is_null(row) {
            return Ok(None); // No PNR data
        }
        let pnr = pnr_array.value(row).to_string();

        // Create the Income record
        Ok(Some(Income::new(
            pnr,
            year,
            adjusted_amount,
            income_type.as_str().to_string(),
        )))
    }

    /// Create Income records for "Other" income type (derived from total and salary)
    pub fn from_ind_record_derived_other(
        batch: &RecordBatch,
        row: usize,
        year: i32,
        adjust_inflation: Option<&dyn Fn(f64) -> f64>,
    ) -> Result<Option<Income>> {
        // Extract PNR column
        let pnr_array_opt = get_column(batch, "PNR", &DataType::Utf8, true)?;
        let pnr_array = match &pnr_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "PNR", "String")?,
            None => return Ok(None), // Required column missing
        };

        // Extract total income
        let total_array_opt = get_column(batch, "PERINDKIALT_13", &DataType::Float64, false)?;
        let total_income = match &total_array_opt {
            Some(array) => {
                let float_array = downcast_array::<Float64Array>(&array, "PERINDKIALT_13", "Float64")?;
                if row < float_array.len() && !float_array.is_null(row) {
                    float_array.value(row)
                } else {
                    return Ok(None); // No total income data
                }
            }
            None => return Ok(None), // Column not found
        };

        // Extract salary income
        let salary_array_opt = get_column(batch, "LOENMV_13", &DataType::Float64, false)?;
        let salary_income = match &salary_array_opt {
            Some(array) => {
                let float_array = downcast_array::<Float64Array>(&array, "LOENMV_13", "Float64")?;
                if row < float_array.len() && !float_array.is_null(row) {
                    float_array.value(row)
                } else {
                    return Ok(None); // No salary data
                }
            }
            None => return Ok(None), // Column not found
        };

        // Calculate other income
        let other_income = total_income - salary_income;
        if other_income <= 0.0 {
            return Ok(None); // No positive "other" income
        }

        // Apply inflation adjustment if provided
        let adjusted_amount = match adjust_inflation {
            Some(adjust_fn) => adjust_fn(other_income),
            None => other_income,
        };

        // Get PNR
        if row >= pnr_array.len() || pnr_array.is_null(row) {
            return Ok(None); // No PNR data
        }
        let pnr = pnr_array.value(row).to_string();

        // Create the Income record
        Ok(Some(Income::new(
            pnr,
            year,
            adjusted_amount,
            IncomeType::Other.as_str().to_string(),
        )))
    }
    
    /// Create Income models from an entire IND record batch for a specific year
    pub fn from_ind_batch(
        batch: &RecordBatch,
        year: i32,
        adjust_inflation: Option<&dyn Fn(f64, i32, i32) -> f64>,
    ) -> Result<Vec<Income>> {
        let mut incomes = Vec::new();
        
        // Process each row in the batch
        for row in 0..batch.num_rows() {
            // Adapter function for inflation adjustment
            let adjust_fn = adjust_inflation.map(|f| {
                move |amount: f64| f(amount, year, year) // No year adjustment needed here
            });
        
            // Add total income
            if let Ok(Some(income)) = Self::from_ind_record(
                batch, 
                row, 
                year, 
                &IncomeType::TotalPersonal, 
                adjust_fn.as_ref().map(|f| f as &dyn Fn(f64) -> f64)
            ) {
                incomes.push(income);
            }
            
            // Add salary income
            if let Ok(Some(income)) = Self::from_ind_record(
                batch, 
                row, 
                year, 
                &IncomeType::Salary, 
                adjust_fn.as_ref().map(|f| f as &dyn Fn(f64) -> f64)
            ) {
                incomes.push(income);
            }
            
            // Add derived "other" income
            if let Ok(Some(income)) = Self::from_ind_record_derived_other(
                batch, 
                row, 
                year, 
                adjust_fn.as_ref().map(|f| f as &dyn Fn(f64) -> f64)
            ) {
                incomes.push(income);
            }
        }
        
        Ok(incomes)
    }
}