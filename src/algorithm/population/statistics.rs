//! Population statistics and analysis functions
//!
//! This module provides functions for analyzing population data,
//! calculating statistics, and generating summaries.

use std::collections::HashMap;

use crate::models::family::{FamilyCollection, FamilySnapshot, FamilyType};

/// Functions for population statistics and summaries
pub struct PopulationStatistics;

impl PopulationStatistics {
    /// Calculate basic statistics for a family collection
    #[must_use]
    pub fn calculate_basic_stats(
        collection: &FamilyCollection,
        index_date: &chrono::NaiveDate,
    ) -> PopulationStats {
        let individual_count = collection.individual_count();
        let family_count = collection.family_count();
        
        // Get snapshots at the index date for more detailed statistics
        let snapshots = collection.get_snapshots_at(index_date);
        
        let child_count = snapshots.iter().map(|s| s.children.len()).sum();
        
        let two_parent_family_count = snapshots
            .iter()
            .filter(|s| s.mother.is_some() && s.father.is_some())
            .count();
            
        let scd_family_count = snapshots.iter().filter(|s| s.has_child_with_scd()).count();
        
        PopulationStats {
            individual_count,
            family_count,
            child_count,
            two_parent_family_count,
            scd_family_count,
        }
    }
    
    /// Generate a detailed population summary
    #[must_use]
    pub fn generate_summary(
        stats: &PopulationStats,
        index_date: &chrono::NaiveDate,
        case_families: &[FamilySnapshot],
        control_families: &[FamilySnapshot],
    ) -> String {
        let mut summary = String::new();
        summary.push_str("Study Population Summary:\n");
        summary.push_str(&format!("  Index Date: {}\n", index_date));
        summary.push_str(&format!("  Total Individuals: {}\n", stats.individual_count));
        summary.push_str(&format!("  Total Families: {}\n", stats.family_count));
        summary.push_str(&format!("  Total Children: {}\n", stats.child_count));
        summary.push_str(&format!(
            "  Two-Parent Families: {}\n",
            stats.two_parent_family_count
        ));
        summary.push_str(&format!("  Families with SCD: {}\n", stats.scd_family_count));

        // Calculate eligibility counts
        let case_count = case_families.len();
        let control_count = control_families.len();
        
        summary.push_str(&format!("  Eligible Case Families: {case_count}\n"));
        summary.push_str(&format!("  Eligible Control Families: {control_count}\n"));

        // Add family composition details for case families
        if !case_families.is_empty() {
            let mut scd_by_type = HashMap::new();
            let mut total_scd_children = 0;

            // Calculate SCD distribution by family type
            for family in case_families {
                let scd_children = family
                    .children
                    .iter()
                    .filter(|child| child.had_scd_at(index_date))
                    .count();

                total_scd_children += scd_children;

                *scd_by_type.entry(family.family_type).or_insert(0) += 1;
            }

            // Calculate average children per case family
            let avg_children_per_case = if case_count > 0 {
                case_families
                    .iter()
                    .map(|f| f.children.len())
                    .sum::<usize>() as f64
                    / case_count as f64
            } else {
                0.0
            };

            summary.push_str("\nCase Family Characteristics:\n");
            summary.push_str(&format!("  Total SCD Children: {total_scd_children}\n"));
            summary.push_str(&format!(
                "  Average Children per Family: {avg_children_per_case:.2}\n"
            ));

            // Add family type distribution
            summary.push_str("  Distribution by Family Type:\n");
            for (family_type, count) in scd_by_type {
                let type_label = match family_type {
                    FamilyType::TwoParent => "Two-Parent",
                    FamilyType::SingleMother => "Single Mother",
                    FamilyType::SingleFather => "Single Father",
                    FamilyType::NoParent => "No Parent",
                    FamilyType::Unknown => "Unknown",
                };

                let percentage = if case_count > 0 {
                    (count as f64 / case_count as f64) * 100.0
                } else {
                    0.0
                };

                summary.push_str(&format!("    {type_label}: {count} ({percentage:.1}%)\n"));
            }
        }

        // Add control family details
        if !control_families.is_empty() {
            // Calculate average children per control family
            let avg_children_per_control = if control_count > 0 {
                control_families
                    .iter()
                    .map(|f| f.children.len())
                    .sum::<usize>() as f64
                    / control_count as f64
            } else {
                0.0
            };

            summary.push_str("\nControl Family Characteristics:\n");
            summary.push_str(&format!(
                "  Average Children per Family: {avg_children_per_control:.2}\n"
            ));
        }

        // Add matching potential estimate
        if case_count > 0 && control_count > 0 {
            let ratio = control_count as f64 / case_count as f64;
            summary.push_str("\nMatching Potential:\n");
            summary.push_str(&format!("  Control-to-Case Ratio: {ratio:.2}:1\n"));

            // Suggest potential matching strategies
            summary.push_str("  Recommended Matching Approaches:\n");
            if ratio >= 3.0 {
                summary.push_str("    - 3:1 matching feasible with strict criteria\n");
            } else if ratio >= 1.0 {
                summary.push_str("    - 1:1 matching with optimized criteria\n");
            } else {
                summary
                    .push_str("    - Consider relaxing case criteria or population restrictions\n");
            }
        }

        summary
    }
}

/// Structure containing basic population statistics
#[derive(Debug, Clone)]
pub struct PopulationStats {
    /// Total number of individuals in the population
    pub individual_count: usize,
    /// Total number of families in the population
    pub family_count: usize,
    /// Number of children in the population
    pub child_count: usize,
    /// Number of families with both parents present
    pub two_parent_family_count: usize,
    /// Number of families with severe chronic disease
    pub scd_family_count: usize,
}