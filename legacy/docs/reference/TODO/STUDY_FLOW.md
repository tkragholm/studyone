# Study Flow

## Background and Objectives

This study investigates the long-term impact of having a child with a severe chronic disease (SCD) on parental income trajectories in Denmark over a 22-year period (2000-2022). Using a matched case-control methodology with a difference-in-differences approach, we compare income trajectories between parents of children with severe chronic diseases and parents of children without such conditions.

### Specific Aims

1. Quantify differences in income trajectories between parents of children with SCD and control parents
2. Examine gender differences in impact between mothers and fathers
3. Explore how impacts vary by disease type and severity, categorizing conditions as:
   - **Mild**: Conditions like asthma
   - **Moderate**: Remaining conditions in the SCD algorithm
   - **Severe**: More serious diagnoses like cancer, organ transplantation, and chromosomal anomalies
4. Investigate how socioeconomic factors moderate these impacts

### Hypotheses

- Parents of children with SCD will experience significantly lower income trajectories
- Effects will be more pronounced for mothers than fathers
- Impact will vary by disease type and severity
- Pre-existing socioeconomic factors will moderate observed effects

This research follows the Strengthening the Reporting of Observational Studies in Epidemiology (STROBE) guidelines to ensure transparency in reporting.

## Study Design

This registry-based case-control study follows five major steps:

1. Population generation from demographic registers
2. Identification of severe chronic diseases using the SCD algorithm
3. Matching of cases (children with SCD) with controls (children without SCD)
4. Covariate balance assessment
5. Analysis of parental income trajectories using difference-in-differences methodology

This design minimizes confounding biases through careful matching while allowing for efficient analysis of the relationship between childhood chronic disease and parental economic outcomes.

## Unit of Analysis

We employ three complementary units of analysis:

1. **Individual Parent**: Primary unit for examining income trajectories and labor market outcomes, enabling direct gender comparisons between mothers and fathers

2. **Family Unit**: For analyzing combined household income and family-level economic impacts, recognizing that work and caregiving decisions often occur at the household level

3. **Sibling Context**: Accounting for family composition effects through:
   - Number of siblings
   - Birth order
   - Presence of multiple children with chronic conditions
   - Age differences between siblings

This multi-level approach provides a comprehensive understanding of how childhood chronic disease affects both individual parents and family systems.

## Data Sources

The study utilizes several national registers as data sources:

### Demographic Registers

**BEF**: Population Register with demographic variables. Contains information about individuals and their parents (identified through FAR_ID or MOR_ID). Key columns include:

- PNR: Personal identification number
- KOEN: Gender
- FOED_DAG: Birth date
- FAR_ID: Father's identification number
- MOR_ID: Mother's identification number
- FAMILIE_ID: Family identifier

**MFR**: Medical Birth Register with health-related variables around birth. Key columns include:

- CPR_BARN: Child's personal identification number
- FOEDSELSDATO: Birth date
- CPR_MODER: Mother's personal identification number
- CPR_FADER: Father's personal identification number

### Administrative Registers

**VNDS**: Emigration Register with migration information. Key columns include:

- PNR: Personal identification number
- HAEND_KODE: Event code (immigration or emigration)
- HAEND_DATO: Event date

**DOD**: Death Register with mortality information. Key columns include:

- PNR: Personal identification number
- DODDATO: Date of death

**Income Registers**: Containing detailed income data for the 2000-2022 period, allowing for analysis of income trajectories over time.

**Population Education Register (PER)**: Contains educational attainment information with 8-digit education codes that can be converted to ISCED levels.

### Health Registers

#### LPR2 (Legacy Format, valid until 2018)

- Three primary components:
  - **Admissions** (`LPR_ADM`): Patient hospital admissions with dates, primary diagnosis
  - **Diagnoses** (`LPR_DIAG`): Detailed diagnosis codes linked to admissions
  - **Visits** (`LPR_BES`): Outpatient data linked to admissions
- Key identifier: `RECNUM` links records across tables

#### LPR3 (Current Format, from 2019)

- Two primary components:
  - **Contacts** (`LPR3_KONTAKTER`): Patient contacts with healthcare system
  - **Diagnoses** (`LPR3_DIAGNOSER`): Detailed diagnosis codes for each contact
- Key identifier: `DW_EK_KONTAKT` links records across tables

## Data Processing Pipeline

```
┌───────────────┐     ┌─────────────────┐     ┌────────────────┐
│ Data Loading  │────▶│ Data Integration│────▶│ Data           │
└───────────────┘     └─────────────────┘     │ Harmonization  │
                                              └────────┬───────┘
┌───────────────┐     ┌─────────────────┐              │
│ Analytics     │◀────│ SCD Algorithm   │◀─────────────┘
│ Generation    │     │ Application     │
└───────────────┘     └─────────────────┘
```

1. **Data Loading**:
   - Loading data from various registers (BEF, MFR, LPR2/LPR3, VNDS, DOD, Income)
   - Implemented in `src/data/registry/loaders` for each data source
   - Uses DataFusion-based providers for efficient data access

2. **Data Integration**:
   - `integrate_lpr2_components()`: Joins LPR2 tables on RECNUM
   - `integrate_lpr3_components()`: Joins LPR3 tables on DW_EK_KONTAKT
   - Integrates demographic data from BEF and MFR using common identifiers (PNR)
   - Links parental income data to child health records

3. **Data Harmonization**:
   - `harmonize_health_data()`: Standardizes column names across LPR2 and LPR3
   - Field mappings include:
     - Patient IDs: `PNR`/`CPR` → `patient_id`
     - Diagnoses: `C_ADIAG`/`aktionsdiagnose` → `primary_diagnosis`
     - Dates: `D_INDDTO`/`dato_start` → `admission_date`
   - Standardizes income variables across years to ensure comparability

4. **Data Combination**:
   - `combine_harmonized_data()`: Combines LPR2 and LPR3 into unified dataset
   - Handles schema matching, data type conversion, null handling
   - Creates a comprehensive dataset linking children, parents, and income data

5. **SCD Algorithm Application**:
   - `apply_scd_algorithm()`: Applies the Severe Chronic Disease algorithm
   - Identifies 10 major disease categories (including blood disorders, immune system disorders, etc.)
   - Further categorizes conditions by severity (mild, moderate, severe) based on study criteria
   - Implemented in `src/algorithm/health/diagnosis/scd.rs`
   - Uses optimized parallel processing for large datasets

6. **Analytics Generation**:
   - Creates various analytical views (longitudinal summaries, group analysis)
   - Generates balance reports for matched cases and controls
   - Prepares data for difference-in-differences analysis of parental income trajectories

## Population Generation

The population generation process is implemented in `src/algorithm/population/core.rs` and follows these steps:

1. **Define the study population**

   From BEF:
   - Collect individuals born between specified years (e.g., 1995 to 2018, inclusive)
   - Include columns: PNR, KOEN, FOED_DAG, FAR_ID, MOR_ID, FAMILIE_ID

   From MFR:
   - Collect individuals born in the same time period
   - Include columns: CPR_BARN, FOEDSELSDATO, CPR_MODER, CPR_FADER

2. **Combine demographic data**
   - Cross-reference between BEF and MFR to create comprehensive demographic profiles
   - Resolve inconsistencies and ensure data completeness
   - Handle missing parents using optimized data structures
   - Create a unified population dataset with standardized columns

3. **Identify and link siblings**
   - Use FAMILIE_ID and parent identifiers to identify siblings within families
   - Create sibling relationships and document birth order
   - Record presence of siblings with severe chronic conditions
   - Calculate age differences between siblings
   - Determine family size (number of children)

4. **Assess migration status**

   From VNDS:
   - Cross-check children and parents with migration records
   - Identify emigration and immigration patterns
   - Flag individuals who have moved out of the country

5. **Determine mortality status**

   From DOD:
   - Cross-check children and parents with death records
   - Identify deceased individuals and their dates of death

6. **Incorporate parental income data**
   - Link parental income records to the population dataset
   - Ensure continuous coverage over the 22-year study period (2000-2022)
   - Handle missing income data and outliers
   - Create both individual and combined household income variables

## Augmenting the Population with Health Data

The process of adding health information to the study population is managed by `src/commands/population_scd/handler.rs`:

1. **Load and prepare health data**
   - Load LPR2 and/or LPR3 data based on configuration
   - Harmonize data formats across different health register versions
   - Filter relevant time periods according to study parameters

2. **Apply the Severe Chronic Disease (SCD) algorithm**
   - Process each individual's diagnosis codes using `apply_scd_algorithm()`
   - Identify 10 major disease categories:
     - Blood disorders
     - Immune system disorders
     - Endocrine disorders
     - Neurological disorders
     - Cardiovascular disorders
     - Respiratory disorders
     - Gastrointestinal disorders
     - Musculoskeletal disorders
     - Renal disorders
     - Congenital disorders
   - Flag patients with SCD and record first diagnosis date
   - Store disease category information for each patient

3. **Categorize disease severity**
   - Classify conditions using multiple approaches:
     1. **Categorical approach**:
        - Mild (e.g., asthma)
        - Moderate (remaining conditions in SCD algorithm)
        - Severe (e.g., cancer, organ transplantation, chromosomal anomalies)
     2. **Origin-based classification**:
        - Congenital vs. Acquired (based on ICD-10 codes and clinical characteristics)
     3. **Utilization-based approach**:
        - Using hospitalization frequency (≥5 hospitalizations/year = severe)
     4. **Combined severity score**:
        - Integrating multiple classification methods
   - Generate severity indicators for subsequent analysis

4. **Generate SCD indicators**
   - Create binary indicators for SCD status
   - Calculate time-to-diagnosis variables
   - Create category-specific indicators

## Covariates and Temporal Considerations

### Covariate Measurement Timing

To avoid introducing bias through inappropriate adjustment for mediators, we will strictly adhere to the following timing principles for covariate measurement:

1. **Pre-exposure measurement only**: All covariates will be measured at or before the index date (date of SCD diagnosis for cases or equivalent matching date for controls).

2. **Baseline establishment**: For income trajectory analysis, a clear "pre-exposure" baseline period will be established using data from before the index date.

3. **No post-exposure adjustment**: Variables that could be affected by having a child with SCD (potential mediators) will not be included as covariates in the primary analysis.

This approach prevents us from inappropriately controlling for variables in the causal pathway between exposure (having a child with SCD) and outcome (parental income), which would bias our estimates of the total effect.

### Selected Covariates

The following covariates are included in the analysis, all measured at or before the index date:

1. **Demographic variables**:
   - Parental age: Continuous variable measured in years at child's birth
   - Parental education level: Categorized using ISCED conversion script (low: ISCED 0-2, medium: ISCED 3-4, high: ISCED 5-8), measured at index date
   - Family size: Discrete variable indicating number of children in family at index date
   - Geographical location: Municipality code and rural/urban classification at index date
   - Child's birth year: Discrete variable
   - Parents' relationship status: Binary variable indicating whether parents are living together at index date
   - Immigrant background: Categorized as Danish, Western, or Non-Western

2. **Socioeconomic variables**:
   - Parental pre-exposure income: Continuous variable in Danish Kroner, inflation-adjusted, measured before index date
   - Parental pre-exposure employment status: Binary variable (employed/unemployed) before index date
   - Pre-diagnosis job situation: Categorical variable describing job type before index date

3. **Proxy variables**:
   - Parental comorbidity: From Danish National Patient Register, documented before index date
   - Family support network: Binary variable based on presence of grandparents or adult siblings in same municipality at index date
   - Job market conditions: Approximated using regional unemployment rates at index date

These covariates are used for matching, adjustment in statistical models, and as potential effect modifiers in subgroup analyses.

### Mediator vs. Confounder Analysis

For methodological completeness, we will conduct supplementary analyses to distinguish between:

1. **Total effect models**: Primary analyses that include only pre-exposure covariates, capturing the total effect of having a child with SCD on parental income

2. **Direct effect models**: Secondary analyses that additionally adjust for potential mediators, helping to understand mechanisms through which SCD affects parental income

All mediator analyses will be clearly labeled as exploratory and will not be used to estimate the primary causal effect of interest.

## Case-Control Matching

The case-control matching process is implemented in `src/algorithm/matching.rs` and follows these steps:

1. **Extract cases and controls**
   - Cases: Children identified with SCD
   - Controls: Children without SCD from the same population

2. **Define matching criteria** (`MatchingCriteria` struct):
   - Birth date window (days): Maximum allowed difference in birth dates
   - Parent birth date window (days): Maximum allowed difference in parent birth dates
   - Whether both parents are required for matching
   - Whether same gender is required for matching
   - Family size: Similar number of siblings
   - Socioeconomic factors: To ensure balance in pre-exposure period

3. **Perform optimized matching**
   - Use binary search and indexing for efficient potential match identification
   - Apply parallel processing for large datasets
   - Group cases by birth date ranges for better cache efficiency
   - Track used controls to prevent duplicate matching
   - Randomly select from eligible controls to achieve specified matching ratio

4. **Create matched datasets**
   - Generate datasets of matched cases and controls
   - Save matched pairs for subsequent analysis
   - Ensure parent information is preserved for income trajectory analysis
   - Maintain sibling information for family-level analyses

## Handling Time-Dependent Exposure Status

A critical methodological challenge in this study is the potential for families to change exposure status over time. This occurs when:

1. A family initially serves as a control (no children with SCD) but later has another child who develops SCD
2. A family initially classified as a case (having a child with SCD) is later selected as a control for another case

This time-dependent exposure status presents several potential biases:

### Challenges with Family Dynamics

Several challenges arise from dynamic family structures and changing family composition:

1. **Changing exposure status**: Families initially serving as controls may later have a child with SCD, or a family with a child with SCD might be selected as a control for another case.

2. **Family structure changes**: Parents may divorce, remarry, or form new families during the study period, complicating the tracking of family units over time.

3. **Sibling influences**: The presence of siblings with or without SCD affects the family unit and may influence parental outcomes.

### Fixed-Time Point Approach

To avoid the complexities of time-varying exposures and changing family dynamics, we will employ a fixed-time point approach:

1. **Index case identification**: For each case, clearly identify the specific child with SCD who serves as the "index case" and document their date of diagnosis.

2. **Fixed family structure assessment**: Establish a clear snapshot of the family structure at the time of the index case's diagnosis (or matching date for controls).

3. **Clear eligibility criteria**:
   - A family can only serve as a control if they had no children with SCD as of the matching date
   - Once a family has a child with SCD, they are only eligible to be a case family, never a control
   - Each family can only appear once in the analysis, either as a case or as a control

4. **Family composition documentation**:
   - Document family status (single parent, married parents, etc.) at index date
   - Record all siblings present at index date
   - Track any family structure changes as covariates in the analysis rather than as changing exposure definitions

5. **Post-matching verification**:
   - After initial matching, verify that no selected control families had a child with SCD prior to the matching date
   - Remove any control families that violate this condition and replace them with eligible controls

This approach maintains the integrity of the case-control comparison by ensuring that case and control statuses are clearly defined and do not change throughout the analysis period. By fixing the exposure definition at a specific time point and documenting family structures at that time, we avoid the methodological complexities of time-varying exposures while still accounting for family dynamics as covariates in our analysis.

## Covariate Balance Assessment

After matching, the covariate balance between cases and controls is assessed to ensure the validity of comparisons:

1. Calculate standardized differences for each covariate
2. Generate balance metrics:
   - Total number of covariates
   - Number of imbalanced covariates
   - Maximum standardized difference
   - Mean absolute standardized difference
3. Create detailed balance reports for review
4. Pay particular attention to pre-exposure period income trajectories to validate difference-in-differences assumptions
5. Check for balance in family structure variables, including sibling presence and characteristics

## Income Trajectory Analysis

The analysis of parental income trajectories will follow these steps:

1. **Pre-processing of income data**
   - Standardize income measurements across years
   - Account for inflation and economic changes
   - Handle missing data and outliers
   - Create both individual-level and family-level income variables

2. **Difference-in-differences analysis**
   - Compare income trajectories before and after child's diagnosis
   - Analyze differences between case and control parents
   - Assess gender differences in impact (mothers vs. fathers)
   - Examine effects by disease severity category
   - Account for family structure in models (single vs. dual parent households)

3. **Socioeconomic moderation analysis**
   - Evaluate how pre-existing socioeconomic factors moderate the impact
   - Consider interaction effects between SCD status and socioeconomic variables
   - Assess if certain demographic groups are more vulnerable to income effects

4. **Subgroup analyses**
   - Disease severity: Analysis stratified by:
     - Congenital vs. acquired conditions
     - Hospitalization frequency (≥5/year = severe)
     - Combined severity classification
   - Geographical location: Urban vs. rural analysis
   - Parental education level: Analysis by education strata (low, medium, high)
   - Family structure: Analysis by family size and sibling characteristics

5. **Multiple comparison adjustment**
   - Use Benjamini-Hochberg procedure to control false discovery rate at 5%
   - Report both unadjusted and adjusted p-values for subgroup analyses

6. **Sensitivity analyses**
   - Test robustness of findings to different matching approaches
   - Consider alternative disease severity classifications
   - Examine subgroup analyses by specific disease categories
   - Alternative units of analysis (individual vs. family income)

## Study Management

The entire study pipeline is coordinated by the Study Design handler (`src/commands/study_design/handler.rs`), which:

1. Sets up directory structure for organizing outputs
2. Manages configuration parameters for each processing step
3. Orchestrates the execution of population generation, SCD identification, matching, and balance assessment
4. Provides both synchronous and asynchronous implementations for performance optimization
5. Generates comprehensive logs and progress indicators
6. Creates summary reports of the entire study process

This structured approach ensures reproducibility, efficiency, and comprehensive documentation of the research process in adherence with the STROBE guidelines for reporting observational studies.
