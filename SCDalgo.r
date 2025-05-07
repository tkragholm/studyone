# Severe Chronic Disease Algorithm
# Last edit: 07/09/23

require(tidyverse)
require(data.table)

# Flag SCD entries -------------------------------------------------------------
SCD.flag <- function(tab, d) {
    work_tab <- tab %>%
        mutate(code = str_to_upper(!!sym(d))) %>% # Convert to upper case
        mutate(
            diag4 = str_sub(code, 2, 5),
            diag3 = str_sub(code, 2, 4)
        ) %>% # Subset first positions of the ICD10-code, remove the initial "D"
        mutate(scd = fcase(
            (diag3 >= "C00" & diag3 <= "C99") | # Malignant neoplasms
                str_detect(diag4, "^D61[0389]") | # Aplastic anaemias
                diag4 == "D762" | # Haemophagocytic syndrome, infection-associated
                str_detect(diag3, "^D8[012]") | # Immunodeficiencies
                diag3 == "E10" | # Insulin-dependent diabetes mellitus
                diag3 == "E25" | # Adrenogenital disorders
                str_detect(diag3, "^E7[012]") | diag4 == "E730" | # Disorders of amino-acid metabolism
                (diag3 >= "E74" & diag3 <= "E84") | # Disorders of metabolism and cystic fibrosis
                diag3 == "G12" | # Spinal muscular atropy and related syndromes
                str_detect(diag4, "^G31[089]") | diag3 == "G37" | # Degenerative and demyelinating diseases of the nervous system
                diag3 == "G40" | # Epilepsy
                diag3 == "G60" | # Hereditary and idiopathic neoropathy
                diag4 == "G702" | # Congenital and developmental myasthenia
                str_detect(diag4, "^G71[0123]") | # Mitochondrial myopathy
                diag4 == "G736" | # Myopathy in metabolic diseases
                diag3 == "G80" | # Cerebral palsy
                diag4 %in% c("G811", "G821", "G824") | # Spastic conditions
                diag3 == "G91" | # Hydrocephalus
                diag4 == "G941" | # Hydrocephalus in neoplastic disease
                diag3 == "I12" | # Hypertensive renal disease
                diag3 == "I27" | # Pulmonary heart disease
                (diag3 >= "I30" & diag3 <= "I52") | # Other forms of heart disease
                diag4 == "J448" | # Other specified chronic obstructive pulmonary disease
                diag3 == "J84" | # Other interstitial pulmonary disease
                diag3 == "K21" | # Gastro-oesophageal reflux disease
                str_detect(diag3, "^K5[01]") | # Crohn disease (reginal enteritis) and ulcerative colitis
                str_detect(diag3, "^K7[01234567]") | # Diseases of liver
                diag3 == "K90" | # Intestinal malabsorption
                str_detect(diag3, "^M3[012345]") | # Systematic involvement of connective tissue
                str_detect(diag3, "^N0[345]") | # Nephritic syndrome
                diag3 == "N07" | # Hereditary nephropathy, not elsewhere classified
                diag3 == "N13" | # Obstructive and reflux uropathy
                str_detect(diag3, "^N1[89]") | str_detect(diag3, "^N2[567]") | # Chronic kidney disease
                diag3 == "P27" | # Chronic respiratory disease originating in the perinatal period
                diag3 == "P57" | # Kernicterus
                str_detect(diag4, "^P91[012]") | # Disturbances of cerebral status of newborn
                (diag4 >= "P941" & diag4 <= "P949") | # Disorders of muscle tone of newborns
                str_detect(diag3, "^Q0[1234567]") | # Congenital malformations of the nervous system
                str_detect(diag3, "^Q2[0123456]") | # Congenital malformations of the circulatory system
                str_detect(diag3, "^Q3[0123]") | # Congenital malformations of nose, larynx, trachea, bronchus and lung
                str_detect(diag3, "^Q3[4567]") | # Other congenital malformations of respiratory system, cleft lip and cleft plate
                diag3 == "Q39" | str_detect(diag3, "^Q4[01234]") | str_detect(diag4, "^Q45[0123]") | # Congenital malformations of upper alimentary tract, instestine and pancreas
                str_detect(diag3, "^Q6[01234]") | # Congenital malformations of the urinary system
                diag4 == "Q790" | # Congenital diaphragmatic hernia
                str_detect(diag4, "^Q79[23]") | # Exomphalos and gastroschisis
                diag4 == "Q860" | # Fetal alchohol syndrome (dysmorphic)
                diag3 == "Q87" | # Other specified congenital malformation syndromes affecting multiple systems
                str_detect(diag3, "^Q9[0123456789]"),
            1,
            default = 0
        )) %>%
        select(-c(code, diag4, diag3)) # Drop the helper variables

    return(work_tab)
}
