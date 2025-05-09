# Optimal Data Model for Study Flow

## Implementation Progress

- ✅ Core data entities implemented (Individual, Family, Parent, Child, Diagnosis, Income)
- ✅ Registry adapters created for BEF, MFR, LPR, and IND
- ✅ Population generation framework implemented
- ✅ Family relationship handling implemented
- ✅ Registry integration capabilities added
- ✅ Migration and mortality assessment implemented
- ✅ Sibling identification implemented

## Next Steps

- [ ] Add diagnosis classification system for SCD
- [ ] Implement temporal data handling
- [ ] Create matching algorithm for case-control studies
- [ ] Develop income trajectory analysis tools
- [ ] Implement export utilities for R integration

## Core Data Entities

1. **Individual** (Central entity)
   - Primary identifier: `PNR` (Personal identification number)
   - Demographics: Birth date, gender, origin
   - Temporal attributes: Study eligibility periods
   - Death information: Death date (if applicable)
   - Migration information: Immigration/emigration events

2. **Family Unit**
   - Family identifier: `FAMILY_ID`
   - Family composition: Parent-child relationships
   - Household structure: Family type, number of members
   - Sibling relationships: Birth order, age differences

3. **Parent**
   - Primary identifier: `PNR`
   - Relationship to children: `FAR_ID`/`MOR_ID` links
   - Socioeconomic attributes: Education, employment, income trajectories
   - Pre-index attributes: Baseline characteristics before child diagnosis

4. **Child**
   - Primary identifier: `PNR`
   - Parent references: Links to `FAR_ID` and `MOR_ID`
   - Birth information: From MFR registry
   - Disease status: SCD status, diagnosis date, severity category
   - Sibling information: Position in family, age differences

5. **Diagnosis**
   - Diagnosis code: ICD-10 codes
   - Diagnosis type: Primary/secondary
   - Source: LPR2/LPR3
   - Temporal information: Diagnosis date
   - Severity classification: Mild/moderate/severe categorization

6. **Income Trajectory**
   - Individual reference: Parent `PNR`
   - Temporal structure: Year-by-year income measurements
   - Income types: Different income categories
   - Employment status: Linked to income periods
   - Pre/post-diagnosis distinction: Relative to child's diagnosis

## Relationships Between Entities

1. **Family-Individual**: One-to-many relationship between families and individuals
2. **Parent-Child**: Many-to-many relationship with temporal validity periods
3. **Individual-Diagnosis**: One-to-many relationship between individuals and diagnoses
4. **Individual-Income**: One-to-many relationship over time periods
5. **Child-Sibling**: Many-to-many relationship between children via family structure

## Temporal Considerations

1. **Study Timeline**
   - Pre-diagnosis baseline period: 1-3 years before diagnosis
   - Diagnosis index date: Specific date for case-control matching
   - Post-diagnosis follow-up: Up to 22 years (2000-2022)

2. **Measurement Points**
   - Annual income measurements for consistent trajectories
   - Fixed family structure at index date
   - Pre-exposure covariate measurement at or before index date

## Data Integration Strategy

1. **Registry Harmonization Layer**
   - Schema mappings between registries (e.g., BEF/MFR for family relationships)
   - Temporal alignment of different data sources
   - Data type standardization (dates, categorical variables, etc.)

2. **Derived Attributes Layer**
   - Disease severity classification based on multiple criteria
   - Family structure indicators at index date
   - Pre-diagnosis socioeconomic status composites
   - Income trajectory summaries (slopes, relative changes)

3. **Analysis-Ready Datasets**
   - Case-control matched pairs with balanced covariates
   - Longitudinal income data with clear pre/post periods
   - Family-level aggregate datasets for household analysis
   - Subgroup analysis datasets by disease severity and SES

## Implementation Considerations for R

1. **Data Loading and Integration**
   - Use Parquet files as the data exchange format
   - Create structured R data.frames that preserve relationships
   - Implement efficient joining strategies for large datasets

2. **Temporal Data Handling**
   - Use date-based indices for efficient time-series operations
   - Implement period-specific aggregations (pre/post diagnosis)
   - Support for difference-in-differences designs in longitudinal data

3. **Complex Entity Relationships**
   - Implement parent-child-sibling relationship graphs
   - Support family transitions and temporal relationship changes
   - Enable multi-level aggregation (individual → family → group)

4. **Matching and Balance Assessment**
   - Store matching criteria and results for reproducibility
   - Implement covariate balance metrics and visualizations
   - Support sensitivity analyses with different matching approaches

5. **Statistical Analysis Integration**
   - Prepare data structures compatible with DiD estimation
   - Support for multi-level modeling with family clustering
   - Enable stratified analyses by disease severity and SES

## Key Implementation Classes/Structures

1. **StudyPopulation**: Core container for all study individuals with filtering capabilities
2. **FamilyNetwork**: Graph-based structure for family relationships and analysis
3. **IncomeTrajectory**: Time-series based structure for longitudinal income analysis
4. **DiagnosisClassifier**: Rules-based engine for disease severity classification
5. **MatchedCohort**: Container for case-control pairs with balance statistics
6. **TemporalCovariate**: Structure for time-dependent variable management
7. **AnalysisDataset**: Final analysis-ready data with DID structure