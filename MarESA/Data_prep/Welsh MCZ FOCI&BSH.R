### Script to create the Welsh Inshore FOCI and BSH input datasets for the feature level MarESA aggregation
# Uses the EUNIS correlations table, "Infra_circa_LUT", and the NRW responses to biotope present in Welsh waters
# R version 4.0.5
# Package versions - tidyr_1.1.4   writexl_1.4.0 stringr_1.4.0 dplyr_1.0.7   readxl_1.3.1

library(readxl)
library(dplyr)
library(stringr)
library(writexl)
library(tidyr)

inpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/Input datasets/"
outpath<-"//jncc-corpfile/JNCC Corporate Data/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Mapping/Sensitivity aggregations/Feature_level/MCZ_Wales_In/Input dataset/"

### create list of column names to keep ###
FOCI_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "MCZ HOCI")
BSH_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "MCZ BSH")

# Create list of FOCIs or BSHs to select#
list_FOCI<-"Fragile|Sabellaria|seapens|Sea-pen|Seapens|mud habitats|Mud habitats"
list_BSH<-"Subtidal coarse|Subtidal sand|Subtidal mud|Subtidal mixed"

# read in correlation table for UK habitats #
UK_habitats<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx")) %>%
  filter(`UK Habitat`=="TRUE") %>%
  mutate(`JNCC 15.03 name` = str_replace_all(`JNCC 15.03 name`, "  ", " "))
  
# read in depths LUT #
depths<-read.csv(paste0(inpath, "Infra_Circa_LUT.csv")) %>%
  select(`EUNIScomb`, `Depth`)


#### FOCI ####

# select JNCC codes of habitats NRW advised are not in Wales #
NIW_FOCI_list<-read_excel(path = paste0(inpath, "Welsh_Inshore_MCZ_Aggregation_NRWResponse.xlsx"),
                     sheet = "MCZ HOCI correlation_ALL") %>%
  filter(!is.na(`Comments NRW`)) %>%
  pull(`JNCC code`)

# filter FOCI by specified FOCI, remove EUNIS A6, standardise sea-pen spelling, 
FOCI<-UK_habitats %>%
  select(all_of(FOCI_cols)) %>%
  filter(!is.na(`MCZ HOCI`)) %>%
  filter(str_detect(`MCZ HOCI`, pattern = list_FOCI)) %>%
  filter(!str_detect(`EUNIS code 2007`, pattern = "A6")) %>%
  mutate(`MCZ HOCI` = str_replace_all(`MCZ HOCI`, "Sea-pen", "Seapens")) %>%
  rename (FOCI = `MCZ HOCI`, `Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  mutate(FOCI = strsplit(as.character(FOCI), "/")) %>%
  unnest(FOCI) %>%
  mutate(`FOCI` = str_trim(string = `FOCI`, side = "both")) %>%
  filter(str_detect(`FOCI`, pattern = list_FOCI)) %>%
  filter(!`JNCC code` %in% NIW_FOCI_list) %>%                                          # filter habitats not in wales #
  filter(!str_detect(`Classification level`, pattern = "1|2|3"))                       # remove habitats at EUNIS level 1,2,3 #

# Merge depths based on EUNIS code starts at L6 - if no depth in `depths` for code then it at L5 part of code, then level 4 
FOCI<-FOCI %>%
  mutate(`EUNIScomb` = as.character(str_extract_all(`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d\\d"))) %>%
  left_join(depths, by = "EUNIScomb") %>%
  mutate(`EUNIScomb` = if_else(is.na(`Depth`), as.character(str_extract_all(`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d")), `EUNIScomb`)) %>%
  left_join(depths, by = "EUNIScomb") %>%
  select(-`Depth.x`) %>%
  rename(`Depth` = `Depth.y`) %>%
  mutate(`EUNIScomb` = if_else(is.na(`Depth`), as.character(str_extract_all(`EUNIS code`, pattern = "A\\d\\.\\d\\d")), `EUNIScomb`)) %>%
  left_join(depths, by = "EUNIScomb") %>%
  select(-c(`Depth.x`, `EUNIScomb`)) %>%
  rename(`Depth` = `Depth.y`)

### extract EUNIS level 4 string of level 5 codes, remove L4 if child biotope is present ###
l4_remove_FOCI<-unique(str_extract_all(FOCI$`EUNIS code`, pattern = "A\\d\\.\\d\\d(?=\\d$)", simplify = TRUE))
l4_remove_FOCI<-l4_remove_FOCI[l4_remove_FOCI !=""]
FOCI<-FOCI %>%
  filter(!`EUNIS code` %in% l4_remove_FOCI)


### extract the EUNIS level 5 string of level 6 codes, remove L5 if child biotope is present ###
l5_remove_FOCI<-unique(str_extract_all(FOCI$`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d(?=\\d$)", simplify = TRUE))
l5_remove_FOCI<-l5_remove_FOCI[l5_remove_FOCI !=""]
FOCI<-FOCI %>%
  filter(!`EUNIS code` %in% l5_remove_FOCI)

# rearrange columns
FOCI<-FOCI %>%
  select(`FOCI`, `Depth`, everything())

# save to outpath location
write_xlsx(FOCI, path = paste0(outpath, "Welsh_Inshore_FOCI_", Sys.Date(), ".xlsx"))



#### BSH ####

# select JNCC codes of biotopes not in wales
NIW_BSH_list<-read_excel(path = paste0(inpath, "Welsh_Inshore_MCZ_Aggregation_NRWResponse.xlsx"),
                    sheet = "MCZ_BSH_correlation_ALL") %>%
  filter(!is.na(`Comments NRW`)) %>%
  pull(`JNCC code`)

# create data frame filtering by BSH habitats, remove habitats identified as not in Wales, remove EUNIS L1,2,3, habitats #
BSH<-UK_habitats %>%
  select(all_of(BSH_cols)) %>%
  filter(!is.na(`MCZ BSH`)) %>%
  filter(!str_detect(`EUNIS code 2007`, pattern = "A6")) %>%
  filter(str_detect(`MCZ BSH`, pattern = list_BSH)) %>%
  rename (BSH = `MCZ BSH`, `Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  filter(!`JNCC code` %in% NIW_BSH_list) %>%
  filter(!str_detect(`Classification level`, pattern = "1|2|3"))

# Merge depths based on EUNIS code starts at L6 - if no depth in `depths` for code then it at L5 part of code, then level 4 
BSH<-BSH %>%
  mutate(`EUNIScomb` = as.character(str_extract_all(`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d\\d"))) %>%
  left_join(depths, by = "EUNIScomb") %>%
  mutate(`EUNIScomb` = if_else(is.na(`Depth`), as.character(str_extract_all(`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d")), `EUNIScomb`)) %>%
  left_join(depths, by = "EUNIScomb") %>%
  select(-`Depth.x`) %>%
  rename(`Depth` = `Depth.y`) %>%
  mutate(`EUNIScomb` = if_else(is.na(`Depth`), as.character(str_extract_all(`EUNIS code`, pattern = "A\\d\\.\\d\\d")), `EUNIScomb`)) %>%
  left_join(depths, by = "EUNIScomb") %>%
  select(-c(`Depth.x`, `EUNIScomb`)) %>%
  rename(`Depth` = `Depth.y`)


### extract EUNIS level 4 string of level 5 codes, remove L4 if child biotope is present ###
l4_remove_BSH<-unique(str_extract_all(BSH$`EUNIS code`, pattern = "A\\d\\.\\d\\d(?=\\d$)", simplify = TRUE))
l4_remove_BSH<-l4_remove_BSH[l4_remove_BSH !=""]
BSH<-BSH %>%
  filter(!`EUNIS code` %in% l4_remove_BSH)

### extract the EUNIS level 5 string of level 6 codes, remove L5 if child biotope is present ###
l5_remove_BSH<-unique(str_extract_all(BSH$`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d(?=\\d)", simplify = TRUE))
l5_remove_BSH<-l5_remove_BSH[l5_remove_BSH !=""]
BSH<-BSH %>%
  filter(!`EUNIS code` %in% l5_remove_BSH)

# rearrange columns
BSH<-BSH %>%
  select(`BSH`, `Depth`, everything())

# write as excel spreadsheet to outpath location #
write_xlsx(BSH, path = paste0(outpath, "Welsh_Inshore_BSH_", Sys.Date(), ".xlsx"))
