//! Module for handling date parsing and formatting.

use chrono::NaiveDate;
use crate::schema::adapt::types::DateFormatConfig;

/// Parse a date string with multiple format attempts
#[must_use]
pub fn parse_date_string(s: &str, config: &DateFormatConfig) -> Option<NaiveDate> {
    // Try all the provided formats
    for format in &config.date_formats {
        if let Ok(date) = NaiveDate::parse_from_str(s, format) {
            return Some(date);
        }
    }

    // If enabled, try to detect the format based on string patterns
    if config.enable_format_detection {
        if let Some(detected_format) = detect_date_format(s) {
            if let Ok(date) = NaiveDate::parse_from_str(s, &detected_format) {
                return Some(date);
            }
        }
    }

    None
}

/// Try to detect the date format based on string patterns
#[must_use]
pub fn detect_date_format(s: &str) -> Option<String> {
    // Check for ISO-like format with dashes (YYYY-MM-DD)
    if s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
        return Some("%Y-%m-%d".to_string());
    }

    // Check for slashes
    if s.contains('/') {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() == 3 {
            if parts[0].len() == 4 {
                return Some("%Y/%m/%d".to_string()); // YYYY/MM/DD
            } else if parts[2].len() == 4 {
                // Check if first part is likely day or month
                if let Ok(first_num) = parts[0].parse::<u8>() {
                    if first_num > 12 {
                        return Some("%d/%m/%Y".to_string()); // DD/MM/YYYY
                    }
                    // Could be either MM/DD/YYYY or DD/MM/YYYY
                    // Default to European format, but this might need context-specific logic
                    return Some("%d/%m/%Y".to_string());
                }
            }
        }
    }

    // Check for dots (DD.MM.YYYY)
    if s.contains('.') {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 3 && parts[2].len() == 4 {
            return Some("%d.%m.%Y".to_string());
        }
    }

    // Check for compact format (YYYYMMDD)
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        return Some("%Y%m%d".to_string());
    }

    // No recognized format
    None
}