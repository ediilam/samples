
## -----------------------------------------------------------------------------
library(dplyr)
library(ggplot2)
library(lubridate)
library(tidyr)
library(purrr)
library(stringr)


## -----------------------------------------------------------------------------
#list of elsi files
files <- list.files('/scratch/gpfs/el1847/dec2022data/datasets/enrollmentyear/')


## -----------------------------------------------------------------------------
#read into dataframes
elsi_dfs <- lapply(files, readr::read_csv, skip=6)

## -----------------------------------------------------------------------------
#select relevant columns
for (i in 1:length(elsi_dfs)) {
  elsi_dfs[[i]] <-elsi_dfs[[i]] %>% dplyr::select(`School Name`,
                                                  starts_with("State Name [Public School] Latest "),
                                                  starts_with("	
State Name [Public School] 20"),
starts_with("State Abbr [Public School] Latest "),
starts_with("School Name [Public School] 2"),
starts_with("School ID - NCES Assigned"),
starts_with("County Name [Public School] "),
starts_with("County Number [Public School]"),
starts_with("ANSI"),
starts_with("Location"),
starts_with("Start of Year"),
starts_with("Total Students All Grades (Excludes AE) "),
starts_with("American Indian/Alaska Native Students [Public School]"),
starts_with("Asian or Asian/Pacific Islander Students [Public School] "),
starts_with("Hispanic Students [Public School]"),
starts_with("Black or African American Students [Public School]"),
starts_with("White Students [Public School]"),
starts_with("Total Race/Ethnicity [Public School]"),
starts_with("Nat. Hawaiian or Other Pacific Isl. Students [Public School]"),
starts_with("Two or More Races Students [Public School]")
)  %>%
    #add year column
    mutate(year = paste('20',
                             sapply(str_split(names(.)[4], '-'), tail,1),
                             sep=''
                             )) %>%
    relocate(year) %>%
    #remove years from column names to match for bind_rows
    rename_with(~gsub("\\d+", "", .)) %>%
    mutate_all(as.character)
}

## -----------------------------------------------------------------------------
#check all column names
as.data.frame(do.call(rbind,sapply(elsi_dfs, names)))


## -----------------------------------------------------------------------------
enrollment <- bind_rows(elsi_dfs) %>%
  #remove non numeric symbols 
  mutate(across(where(is.character), ~na_if(., "†"))) %>%
  mutate(across(where(is.character), ~na_if(., "‡")))%>%
  mutate(across(where(is.character), ~na_if(., "–"))) %>% 
  mutate_at(names(.)[12:20], as.numeric)


## -----------------------------------------------------------------------------
names(enrollment) <- make.names(str_replace(names(enrollment), '\\[Public School\\]', ''))


## -----------------------------------------------------------------------------
enrollment %>% dplyr::select(names(enrollment)[12:20],year) %>% group_by(year) %>%
  summarise_all(funs(sum(is.na(.))))

## -----------------------------------------------------------------------------
enrollment <- enrollment %>% filter(Total.Students.All.Grades..Excludes.AE....>0) %>%
  mutate(perc_black = Black.or.African.American.Students.../Total.Students.All.Grades..Excludes.AE....) %>%
  mutate(school_name = str_trim(
      str_replace_all(str_to_upper(School.Name), "[^[:alnum:]]", " ")),
    school_name_year = str_trim(
      str_replace_all(str_to_upper(School.Name...), "[^[:alnum:]]", " ")),
    state_name = str_trim(str_to_upper(State.Name..Latest.available.year))) %>%
  mutate_at(vars(year), as.numeric)

## -----------------------------------------------------------------------------
unique_schools <- enrollment %>%
  group_by(school_name, Location.ZIP..., year) %>%
  mutate(n=n()) %>%
  filter(n==1) %>%
  select(-n)
dupl_schools <- enrollment %>%
  group_by(school_name, Location.ZIP..., year) %>%
  mutate(n=n()) %>%
  filter(n>1) %>%
  select(-perc_black) %>%
  summarise(across(everything(),
                   ~ if(is.numeric(.)) sum(., na.rm = TRUE) else first(., order_by=Start.of.Year.Status...))) %>%
  ungroup() %>%
  mutate(perc_black = Black.or.African.American.Students.../Total.Students.All.Grades..Excludes.AE....) %>%
  select(-n, names(unique_schools))


## -----------------------------------------------------------------------------
enrollment <- bind_rows(dupl_schools, unique_schools)


## -----------------------------------------------------------------------------
saveRDS(enrollment, file = "/scratch/gpfs/el1847/dec2022data/processed_data/enrollment_processed.rds")

