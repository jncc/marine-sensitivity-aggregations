### Script to create the EScottish Offshore Annex I input dataset for the feature level MarESA aggregation
# Uses the EUNIS correlations table and deep-sea correlations, Bioregions extract and Deep-sea bioregions processed
# R version 4.0.5
# Package versions - tidyr_1.1.4   writexl_1.4.0 stringr_1.4.0 dplyr_1.0.7   readxl_1.3.1


library(readxl)
library(dplyr)
library(stringr)
library(writexl)
library(tidyr)


inpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/Input datasets/"
outpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/AnxI_Scot_Off/Input dataset/"

# read in correlation table for UK habitats #
UK_habitats<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx")) %>%
  filter(`UK Habitat`=="TRUE") %>%
  mutate(`JNCC 15.03 name` = str_replace_all(`JNCC 15.03 name`, "  ", " "))


## Bioregions
# Create vectors to filter bioregions by
Bioregions<-c("Sub-region 1a", "Sub-region 1b", "Sub-region 6a","Sub-region 7a", "Sub-region 7b (deep-sea)")
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
  select(-"BiotopePresence") %>%
  mutate(`SubregionName` = str_remove_all(`SubregionName`, "\\s\\(deep-sea\\)")) %>%
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

# Extract Annex I habitats and SNH subfeatures from correlations table
Annex1_habs<-c("Reefs", "Submarine structures made by leaking gases", "Submarine structures made from leaking gases")
SNH_subtypes<-c("Bedrock", "Stony", "Biogenic (Cold-water coral reefs)")
Annex1_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "Annex I habitat", "SNH Annex I sub-type")

Scot_Annex1<-UK_habitats %>%
  select(all_of(Annex1_cols)) %>%
  rename (`Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`, `Annex I sub-type` = `SNH Annex I sub-type`) %>%
  mutate(`Annex I habitat` = str_split(as.character(`Annex I habitat`), " / ")) %>%
  unnest(`Annex I habitat`) %>%
  filter(`Annex I habitat` %in% Annex1_habs) %>%
  mutate(`Annex I sub-type` = str_split(as.character(`Annex I sub-type`), " / ")) %>%
  unnest(`Annex I sub-type`) %>%
  filter(!(`Annex I habitat` == "Reefs" & !(`Annex I sub-type` %in% SNH_subtypes))) %>%
  left_join(Bioregions_extract) %>%
  filter(!is.na(`SubregionName`)) %>%
  filter(!(`Classification level` < 4))


# remove parent biotopes if child are present per subregion

pattern_l4<-c("^A\\d\\.\\d\\d(?=\\d$)")
pattern_l5<-c("^A\\d\\.\\d\\d\\d(?=\\d$)")
bioregions_annex1<-unique(Scot_Annex1$SubregionName)

for (i in bioregions_annex1){
  
  l4_remove_df<-Scot_Annex1 %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l4_remove<-unique(str_extract_all(l4_remove_df$`EUNIS code`, pattern = pattern_l4, simplify = TRUE))
  l4_remove<-l4_remove[l4_remove !=""]
  Scot_Annex1<<-Scot_Annex1 %>%
    filter(!(`EUNIS code` %in% l4_remove & `SubregionName` == i))
}


for (i in bioregions_annex1){
  
  l5_remove_df<-Scot_Annex1 %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l5_remove<-unique(str_extract_all(l5_remove_df$`EUNIS code`, pattern = pattern_l5, simplify = TRUE))
  l5_remove<-l5_remove[l5_remove !=""]
  Scot_Annex1<<-Scot_Annex1 %>%
    filter(!(`EUNIS code` %in% l5_remove & `SubregionName` == i))
}




##### Deepsea #####
deepsea_cols<-c("JNCC 15.03 code", "Level", "JNCC 15.03 name", "Annex I habitat", "NCMPA PMF")

Scot_deepsea<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx"), sheet = "JNCC Deep-sea to Listed") %>%
  select(all_of(deepsea_cols)) %>%
  rename (`Classification level` = `Level`, `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  mutate(`Annex I habitat` = str_split(as.character(`Annex I habitat`), " / ")) %>%
  unnest(`Annex I habitat`) %>%
  filter(`Annex I habitat` == "Reefs") %>%
  mutate(`JNCC name` = str_replace_all(`JNCC name`, "  ", " ")) %>%
  mutate(`JNCC name` = if_else(`JNCC code` == "M.ArMB.Ro.MixCor.CorGer", 
                               "Corymorpha, Gersemia, Zoantharia and Heliometra glacialis on Arctic mid bathyal rock and other hard substrata",
                               `JNCC name`)) %>%
  mutate(`JNCC name` = if_else(`JNCC code` == "M.AtUB.Ro.BraCom.DalSep",
                               "Dallina septigera and Macandrevia cranium assemblage on Atlantic upper bathyal rock and other hard substrata",
                               `JNCC name`)) %>%
  mutate(`JNCC name` = if_else(`JNCC code` == "M.AtMB.Ro.BraCom.DalSep",
                               "Dallina septigera and Macandrevia cranium assemblage on Atlantic mid bathyal rock and other hard substrata",
                               `JNCC name`)) %>%
  mutate(`Annex I sub-type` = if_else(!(str_detect(`NCMPA PMF`, pattern = "coral reefs")), "Bedrock/Stony", `NCMPA PMF`)) %>%
  mutate(`Annex I sub-type` = if_else(str_detect(`Annex I sub-type`, pattern = "coral reefs"), "Biogenic (cold-water coral reefs)", `Annex I sub-type`)) %>%
  mutate(`Annex I sub-type` = replace_na(`Annex I sub-type`, "Bedrock/Stony")) %>%
  mutate(`Annex I sub-type` = str_split(as.character(`Annex I sub-type`), "/")) %>%
  unnest(`Annex I sub-type`) %>%
  select(-"NCMPA PMF") %>%
  left_join(Bioregions_deepsea) %>%
  filter(!is.na(`SubregionName`)) %>%
  filter(!(`Classification level` < 4))

# remove parent biotopes if child are present per subregion
JNCC_pattern_deepsea_l4<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
JNCC_pattern_deepsea_l5<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
bioregions_scot_deepsea<-unique(Scot_deepsea$SubregionName)

for (i in bioregions_scot_deepsea){
  
  l4_remove_df<-Scot_deepsea %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l4_remove<-unique(str_extract_all(l4_remove_df$`JNCC code`, pattern = JNCC_pattern_deepsea_l4, simplify = TRUE))
  l4_remove<-l4_remove[l4_remove !=""]
  Scot_deepsea<<-Scot_deepsea %>%
    filter(!(`JNCC code` %in% l4_remove & `SubregionName` == i))
}


for (i in bioregions_scot_deepsea){
  
  l5_remove_df<-Scot_deepsea %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l5_remove<-unique(str_extract_all(l5_remove_df$`JNCC code`, pattern = JNCC_pattern_deepsea_l5, simplify = TRUE))
  l5_remove<-l5_remove[l5_remove !=""]
  Scot_deepsea<<-Scot_deepsea %>%
    filter(!(`JNCC code` %in% l5_remove & `SubregionName` == i))
}



## Join inshore and deepsea
All_scot<-full_join(Scot_Annex1, Scot_deepsea) %>%
  select(`Annex I habitat`, `Annex I sub-type`, `SubregionName`, `Classification level`, everything())

write.csv(All_scot, file = paste0(outpath, "Scottish_Offshore_AnnexI_", Sys.Date(), ".csv"))
  