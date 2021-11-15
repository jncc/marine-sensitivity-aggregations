
library(readxl)
library(dplyr)
library(stringr)
library(writexl)
library(tidyr)

inpath<-"" #where input data is stored
outpath<-"" #where to save output

### create list of column names to keep ###
FOCI_cols<-c("EUNIS code 2007", "EUNIS name 2007", "JNCC 15.03 code", "JNCC 15.03 name", "EUNIS level", "MCZ HOCI")

# read in correlation table for UK habitats #
UK_habitats<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx")) 
  

## Bioregions
# Create vectors to filter bioregions by
Yes_Poss<-c("Yes", "Poss")
Bioregions<-c("Sub-region 1a", "Region 2: Southern North Sea", "Region 3: Eastern Channel", "Sub-region 4a", "Sub-region 4b", "Sub-region 5a", "Sub-region 5b")

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
  left_join(JNCC_EUNIS)

Bioregions_deepsea<-read_excel(path=paste0(inpath,"Region4b,7&8_Bioregions_202110_Final.xlsx"))%>%
  select("JNCC biotope name", "JNCC code", "Sub-region 4b") %>%
  mutate(`BiotopePresence` = str_replace_all(`Sub-region 4b`, "Possible", "Poss")) %>%
  select(-"Sub-region 4b") %>%
  filter(`BiotopePresence` %in% Yes_Poss) %>%
  mutate(`SubregionName` = "Sub-region 4b") %>%
  rename(`JNCC name` = `JNCC biotope name`)


# List of FOCI to extract
list_FOCI<-"Cold-water|seapens|Sea-pen|Subtidal chalk"
Additional_EUNIS_pattern<-c('A4.27$|A4.33$')

# filter FOCI by specified FOCI, remove EUNIS level 6, standardise sea-pen spelling #

Additional_EUNIS<-UK_habitats[(str_detect(UK_habitats$`EUNIS code 2007`, pattern = Additional_EUNIS_pattern)),]

FOCI<-UK_habitats %>%
  filter(`UK Habitat` == "TRUE") %>%
  rbind(Additional_EUNIS) %>%
  select(all_of(FOCI_cols)) %>%
  filter(!is.na(`MCZ HOCI`)) %>%
  filter(str_detect(`MCZ HOCI`, pattern = list_FOCI)) %>%
  filter(!(str_detect(`EUNIS code 2007`, pattern = "A6"))) %>%
  rename (FOCI = `MCZ HOCI`, `Classification level` = `EUNIS level`, `EUNIS code` = `EUNIS code 2007`, `EUNIS name` = `EUNIS name 2007`,
          `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  left_join(Bioregions_extract) %>%
  filter(!is.na(`BiotopePresence`))
FOCI<-FOCI %>%
  mutate(FOCI = strsplit(as.character(FOCI), "/")) %>%
  unnest(FOCI) %>%
  mutate(`FOCI` = str_trim(string = `FOCI`, side = "both")) %>%
  filter(str_detect(`FOCI`, pattern = list_FOCI))
FOCI<-FOCI %>%
  mutate(`FOCI` = str_replace(`FOCI`, "seapens", "Sea-pen")) %>%
  mutate(`FOCI` = str_replace(`FOCI`, "Cold-water coral reef", "Cold-water coral reefs"))


# remove parent biotopes if child are present per subregion

pattern_JNCC_l4<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
pattern_JNCC_l5<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
bioregions_FOCI<-unique(FOCI$SubregionName)

for (i in bioregions_FOCI){
  
  l4_remove_df<-FOCI %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l4_remove<-unique(str_extract_all(l4_remove_df$`JNCC code`, pattern = pattern_JNCC_l4, simplify = TRUE))
  l4_remove<-l4_remove[l4_remove !=""]
  FOCI<<-FOCI %>%
    filter(!(`JNCC code` %in% l4_remove & `SubregionName` == i))
}


for (i in bioregions_FOCI){
  
  l5_remove_df<-FOCI %>%
    filter(str_detect(`SubregionName`, pattern = i))
  l5_remove<-unique(str_extract_all(l5_remove_df$`JNCC code`, pattern = pattern_JNCC_l5, simplify = TRUE))
  l5_remove<-l5_remove[l5_remove !=""]
  FOCI<<-FOCI %>%
    filter(!(`JNCC code` %in% l5_remove & `SubregionName` == i))
}



# read in correlation table for UK Deep Sea habitats #
DeepSea_cols<-c("JNCC 15.03 code", "Level", "JNCC 15.03 name", "MCZ BSH", "MCZ HOCI")
DeepSea_FOCI<-c("Cold-water|Coral gardens|Seapens|Sea-pen")

# Deepsea habitats filtered by FOCI
DeepSea_habitats_FOCI<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx"), sheet = "JNCC Deep-sea to Listed") %>%
  select(all_of(DeepSea_cols)) %>%
  filter(str_detect(`MCZ HOCI`, pattern = DeepSea_FOCI)) %>%
  mutate(`MCZ HOCI` = str_replace(`MCZ HOCI`, "Seapens", "Sea-pen")) %>%
  mutate(`MCZ HOCI` = if_else(`JNCC 15.03 code` == "M.AtMB.Bi.CorRee.LopFra", "Cold-water coral reefs or Coral gardens", `MCZ HOCI`)) #change specific biotope to include additional FOCI

DeepSea_habitats_FOCI<-DeepSea_habitats_FOCI %>%
  rename (FOCI = `MCZ HOCI`, `Classification level` = `Level`, `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  mutate(FOCI = strsplit(as.character(FOCI), " or |/")) %>%
  unnest(FOCI) %>%
  mutate(`FOCI` = str_trim(string = `FOCI`, side = "both")) %>%
  filter(str_detect(`FOCI`, pattern = DeepSea_FOCI)) %>%
  filter(!`Classification level` == "2") %>%
  filter(!`Classification level` == "3")


# Deepsea habitats filtered by BSH
DeepSea_habitats_BSH<-read_excel(path=paste0(inpath, "201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx"), sheet = "JNCC Deep-sea to Listed") %>%
  filter(`MCZ BSH` == "Deep-sea bed") %>%
  select(all_of(DeepSea_cols)) %>%
  rename (FOCI = `MCZ HOCI`, `Classification level` = `Level`, `JNCC code` = `JNCC 15.03 code`, `JNCC name` = `JNCC 15.03 name`) %>%
  filter(!`Classification level` == "2") %>%
  filter(!`Classification level` == "3") %>%
  mutate(FOCI = strsplit(as.character(FOCI), " or |/")) %>%
  unnest(FOCI) %>%
  mutate(`FOCI` = str_trim(string = `FOCI`, side = "both")) %>%
  mutate(`FOCI` = str_replace(`FOCI`, "Seapens", "Sea-pen")) %>%
  mutate(`FOCI` = str_replace(`FOCI`, "Deep-sea sponge aggregations", "Deep-sea sponge aggregation"))

# Join two extracts from deep-sea correlations and join bioregions
Deepsea<-full_join(DeepSea_habitats_FOCI, DeepSea_habitats_BSH) %>%
  left_join(Bioregions_deepsea, by = "JNCC code") %>%
  select(-`JNCC name.y`) %>%
  rename(`JNCC name` = `JNCC name.x`) %>%
  filter(!is.na(`BiotopePresence`))


# remove parent biotopes if child are present - per biotope per subregion (only subregion 4b in deepsea extracts)

JNCC_pattern_deepsea_l4<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
JNCC_pattern_deepsea_l5<-c("^[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+\\.[[:alpha:]]+(?=\\.[[:alpha:]]+$)")
Sub_region4b<-c("Sub-region 4b")

l4_remove_df<-Deepsea %>%
  filter(str_detect(`SubregionName`, pattern = Sub_region4b))
l4_remove<-unique(str_extract_all(l4_remove_df$`JNCC code`, pattern = JNCC_pattern_deepsea_l4, simplify = TRUE))
l4_remove<-l4_remove[l4_remove !=""]
Deepsea<-Deepsea %>%
  filter(!(`JNCC code` %in% l4_remove & `SubregionName` == Sub_region4b))

l5_remove_df<-Deepsea %>%
  filter(str_detect(`SubregionName`, pattern = Sub_region4b))
l5_remove<-unique(str_extract_all(l5_remove_df$`JNCC code`, pattern = JNCC_pattern_deepsea_l5, simplify = TRUE))
l5_remove<-l5_remove[l5_remove !=""]
Deepsea<-Deepsea %>%
  filter(!(`JNCC code` %in% l5_remove & `SubregionName` == Sub_region4b))


# Join dataframes with bioregions from correlations table and deep-sea to listed
Alloffshore_bioregions<-bind_rows(FOCI, Deepsea) %>%
  select(-`BiotopePresence`) %>%
  mutate(`FOCI` = if_else(is.na(`FOCI`), "None", `FOCI`)) %>%
  filter(!(`FOCI` == "Sea-pen and burrowing megafauna communities" & str_detect(string = `EUNIS code`, pattern = "A5.35|A5.36|A5.37"))) %>%
  mutate(`FOCI` = na_if(`FOCI`, "None"))


# rearrange columns
Alloffshore_bioregions<-Alloffshore_bioregions %>%
  select(`FOCI`, `MCZ BSH`, `SubregionName`, `Classification level`, everything())


# write as excel spreadsheet #
write_xlsx(Alloffshore_bioregions, path = paste0(outpath, "English Offshore FOCI&BSH.xlsx"))




  
