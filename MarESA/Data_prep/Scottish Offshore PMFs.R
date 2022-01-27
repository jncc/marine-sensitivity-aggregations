### Script to create the Scottish Offshore PMF input dataset for the feature level MarESA aggregation
# Uses the EUNIS correlations table and deepsea correlations, Bioregions extract, and Deepsea bioregions processed
# R version 4.0.5
# Package versions - tidyr_1.1.4   writexl_1.4.0 stringr_1.4.0 dplyr_1.0.7   readxl_1.3.1


library(readxl)
library(dplyr)
library(stringr)
library(writexl)
library(tidyr)

inpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/Input datasets/"
outpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/NCMPA_Off/Input dataset/"


# read in correlation table for UK habitats #
UK_habitats<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx")) %>%
  filter(`UK Habitat`=="TRUE") %>%
  mutate(`JNCC 15.03 name` = str_replace_all(`JNCC 15.03 name`, "  ", " "))


## Bioregions
# Create vectors to filter bioregions by
Bioregions<-c("Sub-region 1a","Sub-region 1b","Sub-region 6a", "Sub-region 7a", "Sub-region 7b (deep-sea)")
Yes_Poss<-c("Yes", "Poss")

JNCC_EUNIS<-UK_habitats %>%
  select("EUNIS code 2007", "JNCC 15.03 code") %>%
  rename(`EUNIS code` = `EUNIS code 2007`, `JNCC code` = `JNCC 15.03 code`) %>%
  filter(!is.na(`JNCC code`))


Bioregions_extract<-read_excel(path=paste0(inpath, "BioregionsExtract_20210802.xlsx")) %>%
  select("SubregionName", "BiotopePresence", "HabitatCode") %>%
  filter(`BiotopePresence` %in% Yes_Poss) %>%
  filter(`SubregionName` %in% Bioregions) %>%
  rename(`EUNIS code` = `HabitatCode`) %>%
  filter (!str_detect(string = `EUNIS code`, pattern = "A\\d$|A\\d\\.\\d$")) %>%
  filter (!str_detect(string = `EUNIS code`, pattern = "A6")) %>%
  mutate(`SubregionName` = if_else(str_detect(`SubregionName`, "Sub-region 7b"), "Sub-region 7b", `SubregionName`)) %>%
  select(-"BiotopePresence") %>%
  left_join(JNCC_EUNIS)

Bioregions_deepsea<-read_excel(path=paste0(inpath,"Deepsea_bioregions_processed_2021-11-29.xlsx")) %>%
  select("JNCC biotope name", "JNCC code", "Sub-region 7b", "Region 8") %>%
  mutate(`Sub-region 7b` = str_replace_all(`Sub-region 7b`, "Yes|Possible", "Sub-region 7b")) %>%
  mutate(`Region 8` = str_replace_all(`Region 8`, "Yes|Possible", "Region 8")) %>%
  unite(col = "SubregionName", all_of(c("Sub-region 7b", "Region 8")), sep = "/", na.rm = TRUE) %>%
  mutate(`SubregionName` = strsplit(as.character(`SubregionName`), "/")) %>%
  unnest(`SubregionName`) %>%
  mutate(`SubregionName` = str_trim(string = `SubregionName`, side = "both")) %>%
  filter(!`SubregionName` == "No") %>%
  rename(`JNCC name` = `JNCC biotope name`) %>%
  mutate(`JNCC name` = str_replace_all(`JNCC name`, "  ", " "))


#### PMF ####

# Create vectors of habitats to keep, and column to select from correlations table
PMF_list<-c("Burrowed mud|Cold-water coral reefs|Offshore deep sea mud|Offshore deep-sea mud|Offshore subtidal sands and gravels")
PMF_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "Priority Marine Feature (PMF) (Scotland)")

PMF<-UK_habitats %>%
  select(all_of(PMF_cols)) %>%
  filter(!is.na(`Priority Marine Feature (PMF) (Scotland)`)) %>%
  rename (`PMF` = `Priority Marine Feature (PMF) (Scotland)`, `Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  mutate(`PMF` = str_replace_all(`PMF`, "Offshore deep sea muds", "Offshore deep sea mud")) %>%
  mutate(`PMF` = str_replace_all(`PMF`, "Offshore deep sea mud", "Offshore deep-sea mud")) %>%
  mutate(`PMF` = str_replace_all(`PMF`, "deepsea", "deep sea")) %>%
  filter(str_detect(`PMF`, pattern = PMF_list)) %>%
  mutate(`PMF` = strsplit(as.character(`PMF`), " or |/")) %>%
  unnest(`PMF`) %>%
  mutate(`PMF` = str_trim(string = `PMF`, side = "both")) %>%
  filter(str_detect(`PMF`, pattern = PMF_list)) %>%
  filter(! str_detect(`EUNIS code`, pattern = "A6")) %>%
  filter(!`Classification level` == "2") %>%
  filter(!`Classification level` == "3") %>%
  left_join(Bioregions_extract) %>%
  filter(!is.na(`SubregionName`))


# remove parent biotopes if child are present per subregion
pattern_JNCC_l4<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
pattern_JNCC_l5<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
bioregions_PMF<-unique(PMF$SubregionName)

for (i in bioregions_PMF){
  
  l4_remove_df<-PMF %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l4_remove<-unique(str_extract_all(l4_remove_df$`JNCC code`, pattern = pattern_JNCC_l4, simplify = TRUE))
  l4_remove<-l4_remove[l4_remove !=""]
  PMF<<-PMF %>%
    filter(!(`JNCC code` %in% l4_remove & `SubregionName` == i))
}


for (i in bioregions_PMF){
  
  l5_remove_df<-PMF %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l5_remove<-unique(str_extract_all(l5_remove_df$`JNCC code`, pattern = pattern_JNCC_l5, simplify = TRUE))
  l5_remove<-l5_remove[l5_remove !=""]
  PMF<<-PMF %>%
    filter(!(`JNCC code` %in% l5_remove & `SubregionName` == i))
}






# NCMPA PMF
NCMPA_PMF_cols<-c("JNCC 15.03 code", "JNCC 15.03 name", "Level", "NCMPA PMF")
NCMPA_PMF_list<-c("Burrowed mud|Cold-water coral reefs|Coral gardens|Offshore deep sea mud|Offshore deep-sea mud|Deep-sea sponge aggregations")

NCMPA_PMF<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx"), sheet = "JNCC Deep-sea to Listed") %>%
  select(all_of(NCMPA_PMF_cols)) %>%
  mutate(`JNCC 15.03 name` = str_replace_all(`JNCC 15.03 name`, "  ", " ")) %>%
  rename (`PMF` = `NCMPA PMF`, `Classification level` = `Level`, `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  mutate(`PMF` = str_replace_all(`PMF`, "aggregation$", "aggregations")) %>%
  mutate(`PMF` = str_replace_all(`PMF`, "Offshore deep sea mud", "Offshore deep-sea mud")) %>%
  mutate(`JNCC name` = if_else(`JNCC code` == "M.ArMB.Ro.MixCor.CorGer", "Corymorpha, Gersemia, Zoantharia and Heliometra glacialis on Arctic mid bathyal rock and other hard substrata", `JNCC name`)) %>%
  mutate(`JNCC name` = str_replace_all(`JNCC name`, "Plinthasterassemblage", "Plinthaster assemblage")) %>%
  filter(str_detect(`PMF`, pattern = NCMPA_PMF_list)) %>%
  mutate(`PMF` = strsplit(as.character(`PMF`), " or |/")) %>%
  unnest(`PMF`) %>%
  mutate(`PMF` = str_trim(string = `PMF`, side = "both")) %>%
  filter(!`Classification level` == "2") %>%
  filter(!`Classification level` == "3") %>%
  left_join(Bioregions_deepsea) %>%
  filter(!is.na(`SubregionName`))



# remove parent biotopes if child are present per subregion
JNCC_pattern_deepsea_l4<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
JNCC_pattern_deepsea_l5<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
bioregions_NCMPA_PMF<-unique(NCMPA_PMF$SubregionName)

for (i in bioregions_NCMPA_PMF){
  
  l4_remove_df<-NCMPA_PMF %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l4_remove<-unique(str_extract_all(l4_remove_df$`JNCC code`, pattern = JNCC_pattern_deepsea_l4, simplify = TRUE))
  l4_remove<-l4_remove[l4_remove !=""]
  NCMPA_PMF<<-NCMPA_PMF %>%
    filter(!(`JNCC code` %in% l4_remove & `SubregionName` == i))
}


for (i in bioregions_NCMPA_PMF){
  
  l5_remove_df<-NCMPA_PMF %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l5_remove<-unique(str_extract_all(l5_remove_df$`JNCC code`, pattern = JNCC_pattern_deepsea_l5, simplify = TRUE))
  l5_remove<-l5_remove[l5_remove !=""]
  NCMPA_PMF<<-NCMPA_PMF %>%
    filter(!(`JNCC code` %in% l5_remove & `SubregionName` == i))
}



# JOin correlations table and deepsea correlations into single dataframe
Scottish_offshore<-full_join(PMF, NCMPA_PMF) %>%
  select("PMF", "SubregionName", everything())

# write as csv #
write.csv(Scottish_offshore, file = paste0(outpath, "Scottish_Offshore_PMF_", Sys.Date(), ".csv"))

