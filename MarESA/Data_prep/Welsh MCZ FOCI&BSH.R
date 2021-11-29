

library(readxl)
library(dplyr)
library(stringr)
library(writexl)

# input inpath
inpath<-""


### create list of column names to keep ###
FOCI_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "MCZ HOCI")
BSH_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "MCZ BSH")

# Create list of FOCIs or BSHs to select#
list_FOCI<-"Fragile|Sabellaria|seapens|Sea-pen|mud habitats|Mud habitats"
list_BSH<-"Subtidal coarse|Subtidal sand|Subtidal mud|Subtidal mixed"

# read in correlation table for UK habitats #
UK_habitats<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx")) %>%
  filter(`UK Habitat`=="TRUE")
  
# read in depths LUT #
depths<-read.csv(paste0(inpath, "Infra_Circa_LUT.csv"))
depths<-depths[,c("EUNIScomb", "Depth")]

#### FOCI ####

# filter FOCI by specified FOCI, remove EUNIS level 6, standardise sea-pen spelling #
FOCI<-UK_habitats[, FOCI_cols]
FOCI<-FOCI[!is.na(FOCI$`MCZ HOCI`),]
FOCI<-FOCI[(str_detect(FOCI$`MCZ HOCI`, pattern = list_FOCI)),]
FOCI<-FOCI[!(str_detect(FOCI$`EUNIS code 2007`, pattern = "A6")),]
FOCI$`MCZ HOCI`<-str_replace(FOCI$`MCZ HOCI`, "seapens", "Sea-pen")
FOCI<-FOCI %>%
  rename (FOCI = `MCZ HOCI`, `Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`)
FOCI<-FOCI %>%
  mutate(FOCI = strsplit(as.character(FOCI), "/")) %>%
  unnest(FOCI) %>%
  mutate(`FOCI` = str_trim(string = `FOCI`, side = "both")) %>%
  filter(str_detect(`FOCI`, pattern = list_FOCI))

# select habitats NRW advised are not in Wales #
NIW_FOCI<-read_excel(path = paste0(inpath, "Welsh_Inshore_MCZ_Aggregation_NRWResponse.xlsx"),
                    sheet = "MCZ HOCI correlation_ALL")
NIW_FOCI<-NIW_FOCI[!is.na(NIW_FOCI$`Comments NRW`),]

# remove habitats using JNCC code #
NIW_FOCI_list<-NIW_FOCI$`JNCC code`
FOCI<-FOCI %>%
  filter(!`JNCC code` %in% NIW_FOCI_list)

# create new column to extracting level 4 parent from level 5 and 6, to merge with depths LUT #
FOCI$EUNIScomb<-str_extract_all(FOCI$`EUNIS code`, pattern = "A\\d\\.\\d\\d?", simplify = TRUE)
FOCI<-merge(FOCI, depths, by="EUNIScomb")

# remove habitats at EUNIS level 1,2,3 #
FOCI<-FOCI[!(str_detect(FOCI$`Classification level`, pattern = "1|2|3")),]


### extract EUNIS level 4 string of level 5 codes ###
l4_remove_FOCI<-unique(str_extract_all(FOCI$`EUNIS code`, pattern = "A\\d\\.\\d\\d(?=\\d$)", simplify = TRUE))
l4_remove_FOCI<-l4_remove_FOCI[l4_remove_FOCI !=""]
FOCI<-FOCI %>%
  filter(!`EUNIS code` %in% l4_remove_FOCI)


### extract the EUNIS level 5 string of level 6 codes ###
l5_remove_FOCI<-unique(str_extract_all(FOCI$`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d(?=\\d$)", simplify = TRUE))
l5_remove_FOCI<-l5_remove_FOCI[l5_remove_FOCI !=""]
FOCI<-FOCI %>%
  filter(!`EUNIS code` %in% l5_remove_FOCI)

FOCI$EUNIScomb<-NULL
FOCI<-FOCI %>%
  select(`Classification level`, everything())

write_xlsx(FOCI, path = paste0(inpath, "FOCI.xlsx"))


#### BSH ####

# create data frame filtering by BSH habitats #
BSH<-UK_habitats[, BSH_cols]
BSH<-BSH[!is.na(BSH$`MCZ BSH`),]
BSH<-BSH[!(str_detect(BSH$`EUNIS code 2007`, pattern = "A6")),]
BSH<-BSH[(str_detect(BSH$`MCZ BSH`, pattern = list_BSH)),]
BSH<-BSH %>% 
  rename (BSH = `MCZ BSH`, `Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) # rename columns#

# select habitats advised by NRW as not in wales and filter by JNCC code #
NIW_BSH<-read_excel(path = paste0(inpath, "Welsh_Inshore_MCZ_Aggregation_NRWResponse.xlsx"),
                    sheet = "MCZ_BSH_correlation_ALL")
NIW_BSH<-NIW_BSH[!is.na(NIW_BSH$`Comments NRW`),]
NIW_BSH_list<-NIW_BSH$`JNCC code`
BSH<-BSH %>%
  filter(!`JNCC code` %in% NIW_BSH_list)
# merge depth classifications from LUT by parent JNCC code #
BSH$EUNIScomb<-str_extract_all(BSH$`EUNIS code`, pattern = "A\\d\\.\\d\\d?", simplify = TRUE)
BSH<-merge(BSH, depths, by="EUNIScomb")

### remove EUNIS levels 1, 2, 3 ###
BSH<-BSH[!(str_detect(BSH$`Classification level`, pattern = "1|2|3")),]

### extract EUNIS level 4 string of level 5 codes ###
l4_remove_BSH<-unique(str_extract_all(BSH$`EUNIS code`, pattern = "A\\d\\.\\d\\d(?=\\d$)", simplify = TRUE))
l4_remove_BSH<-l4_remove_BSH[l4_remove_BSH !=""]
BSH<-BSH %>%
  filter(!`EUNIS code` %in% l4_remove_BSH)

### extract the EUNIS level 5 string of level 6 codes ###
l5_remove_BSH<-unique(str_extract_all(BSH$`EUNIS code`, pattern = "A\\d\\.\\d\\d\\d(?=\\d)", simplify = TRUE))
l5_remove_BSH<-l5_remove_BSH[l5_remove_BSH !=""]
BSH<-BSH %>%
  filter(!`EUNIS code` %in% l5_remove_BSH)

# remove EUNIScomb column and rearrange #
BSH$EUNIScomb<-NULL
BSH<-BSH %>%
  select(`Classification level`, everything())

# write as excel spreadsheet #
write_xlsx(BSH, path = paste0(inpath, "BSH.xlsx"))
