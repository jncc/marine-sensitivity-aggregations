### Script to create the English and Welsh Offshore Annex I input dataset for the feature level MarESA aggregation
# Uses the EUNIS correlations table, EUNIS A1 subfeature correlations, Bioregions extract
# R version 4.0.5
# Package versions - tidyr_1.1.4   writexl_1.4.0 stringr_1.4.0 dplyr_1.0.7   readxl_1.3.1


library(readxl)
library(dplyr)
library(stringr)
library(writexl)
library(tidyr)


inpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/Input datasets/"
outpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/AnxI_EngWales_Off/Input dataset/"

# read in correlation table for UK habitats #
UK_habitats<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx")) %>%
  filter(`UK Habitat`=="TRUE") %>%
  mutate(`JNCC 15.03 name` = str_replace_all(`JNCC 15.03 name`, "  ", " "))


## Bioregions
# Create vectors to filter bioregions by
Bioregions<-c("Sub-region 1a", "Region 2: Southern North Sea", "Region 3: Eastern Channel","Sub-region 4a","Sub-region 4b", "Sub-region 5a", "Sub-region 5b")
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
  left_join(JNCC_EUNIS)



# Correlations table extract of submarine structures made by/from leaking gases - (spelling of habitat standardised)
SSLG_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "Annex I habitat")

SSLG<-UK_habitats %>%
  select(all_of(SSLG_cols)) %>%
  mutate(`Annex I habitat` = str_replace_all(`Annex I habitat`, "Submarine structures made from leaking gases", "Submarine structures made by leaking gases")) %>%
  filter(str_detect(`Annex I habitat`, pattern = "Submarine structures made by leaking gases")) %>%
  rename (`Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`)
  

A1_subfeature<-read_excel(path=paste0(inpath, "EUNIS_A1Subfeature_Correlations.xlsx"), sheet = "EUNIStoA1Subfeatures") %>%
  rename (`EUNIS code` = `EUNISCode`, `EUNIS name` = `EUNISName`, `Annex I sub-feature` = `Subfeature Name`)

Annex1<-full_join(SSLG, A1_subfeature) %>%
  left_join(Bioregions_extract, by = "EUNIS code") %>%
  unite(col = "JNCC code", all_of(c("JNCC code.x", "JNCC code.y")), sep = "/", na.rm = TRUE) %>%
  mutate(`JNCC code` = na_if(`JNCC code`, "")) %>%
  filter(!is.na(`SubregionName`)) %>%
  filter (!str_detect(string = `EUNIS code`, pattern = "A\\d$|A\\d\\.\\d$"))



# Add Annex 1 habitat to Annex 1 subfeatures extracted from correlations table 
#Create look up table for subfeatures to habitats
habitat_subfeature<-data.frame ("Annex I habitat" = c(rep("Reefs", 3), rep("Sandbanks which are slightly covered by sea water all the time", 4), "Submarine structures made by leaking gases"),
                                "Annex I sub-feature" = c("Circalittoral rock", "Subtidal biogenic reefs: Sabellaria spp.", "Subtidal stony reef",
                                                          "Subtidal coarse sediment", "Subtidal sand", "Subtidal mud", "Subtidal mixed sediments", 
                                                          NA),
                                check.names = FALSE)

#left join Annex I habitats and look up table, remove duplicated column and rename 
Annex1<-left_join(Annex1, habitat_subfeature, by = "Annex I sub-feature") %>%
  select(-"Annex I habitat.x") %>%
  rename(`Annex I habitat` = `Annex I habitat.y`)


# remove parent biotopes if child are present per subregion
pattern_l4<-c("^A\\d\\.\\d\\d(?=\\d$)")
pattern_l5<-c("^A\\d\\.\\d\\d\\d(?=\\d$)")
bioregions_Annex1<-unique(Annex1$SubregionName)

for (i in bioregions_Annex1){
  
  l4_remove_df<-Annex1 %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l4_remove<-unique(str_extract_all(l4_remove_df$`EUNIS code`, pattern = pattern_l4, simplify = TRUE))
  l4_remove<-l4_remove[l4_remove !=""]
  Annex1<<-Annex1 %>%
    filter(!(`EUNIS code` %in% l4_remove & `SubregionName` == i))
}


for (i in bioregions_Annex1){
  
  l5_remove_df<-Annex1 %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l5_remove<-unique(str_extract_all(l5_remove_df$`EUNIS code`, pattern = pattern_l5, simplify = TRUE))
  l5_remove<-l5_remove[l5_remove !=""]
  Annex1<<-Annex1 %>%
    filter(!(`EUNIS code` %in% l5_remove & `SubregionName` == i))
}

# rearrange columns
Annex1<-Annex1 %>%
  select(`Annex I habitat`, `Annex I sub-feature`, `SubregionName`, `Classification level`, everything())

# write as excel spreadsheet #
write_xlsx(Annex1, path = paste0(outpath, "English and Welsh Offshore AnnexI_", Sys.Date(), ".xlsx"))
