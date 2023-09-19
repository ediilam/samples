## -----------------------------------------------------------------------------
library(tidyverse)
library(lubridate)
library(purrr)
library(stringr)
library(conflicted)
conflicts_prefer(dplyr::filter())
conflicts_prefer(dplyr::select())


## -----------------------------------------------------------------------------
read_variables <- c("project_id",
                    "project_essay_text",
                    "year",
                    "month",
                    "project_grade_level",
                    "project_resource_category",
                    "matched",
                    "project_state",
                    "success",
                    "diversity",
                    "race",
                    "justice",
                    "coexist",
                    "represent",
                    "lgbt",
                    "disability",
                    "tech",
                    "sport",
                    "project_school_name",
                    "project_school_id",
                    "project_zip",
                    "post",
                    "timec",
                    "essay_wcount",
                    "bin_price",
                    "bin_students_reached",
                    "bin_seq_n",
                    "referred",
                    "fk_read",
                    "vader",
                    "sc_essay_wcount",
                    "sc_bin_price",
                    "sc_bin_students_reached",
                    "sc_bin_seq_n",
                    "sc_referred",
                    "sc_fk_read",
                    "sc_vader",
                    "sc_diversity",
                    "sc_race",
                    "sc_justice",
                    "sc_coexist",
                    "sc_represent",
                    "sc_lgbt",
                    "sc_disability",
                    "sc_tech",
                    "sc_sport"  )


## -----------------------------------------------------------------------------
rdf <- readRDS('/scratch/gpfs/el1847/dec2022data/processed_data/r_regression_data_dropna_uncoded.rds') %>% select(all_of(read_variables))
zipcounty <-  readr::read_csv('/scratch/gpfs/el1847/dec2022data/datasets/yearzip_to_county.csv',
                       col_types="iccddddicc"
                       )
voting <- readRDS('/scratch/gpfs/el1847/dec2022data/datasets/county_pres_processed.rds')


## -----------------------------------------------------------------------------
#restrict years to 2012-2022
rdf <- rdf %>% filter(year>2011)
zipcounty2 <- zipcounty 


## -----------------------------------------------------------------------------
#standardize and format project zipcodes
rdf <- rdf %>% 
  #remove decimals
  mutate(project_zip2 = str_replace(project_zip, "\\.\\d+$", "" )) %>%
  #assume 4 digit numbers missing leading 0 - pad
  mutate(project_zip2 = stringr::str_pad(project_zip2, side='left', width=5, pad='0'))

## -----------------------------------------------------------------------------
#distinct zipcode years in data
proj_zips<- rdf %>% group_by(project_zip2) %>% summarise(n=n())

## -----------------------------------------------------------------------------
#zips that won't match 
proj_zips %>%
  anti_join(zipcounty2, by=c('project_zip2'='zip')) %>%
  distinct(project_zip2)


## -----------------------------------------------------------------------------
zipcounty2 <- zipcounty2 %>%
  inner_join(proj_zips %>% distinct(project_zip2), by=c('zip'='project_zip2'))

## -----------------------------------------------------------------------------
zipjoin <- zipcounty2 %>% dplyr::select(-c(...1, city, state)) %>%
  select(zip, geoid,res_ratio, tot_ratio, year) 


## -----------------------------------------------------------------------------
#join zip to county mappings to voting
joined <- zipjoin %>% 
  left_join(voting, by=c('geoid'='county_fips2')) %>%
  #weight county votes by residential ratio of zip in that county 
  mutate(weighted_2016 = res_ratio*pol2016_var,
         weighted_2020 = res_ratio*pol2020_var,
         weighted_tot_2016 = tot_ratio*pol2016_var,
         weighted_tot_2020 = tot_ratio*pol2020_var)
joined %>% filter(!complete.cases(.)) %>% arrange(county_fips) 


## -----------------------------------------------------------------------------
#check counties with no voting data (116)
novote_counties<-joined %>% filter_all(any_vars(is.na(.))) %>% distinct(zip,geoid)

## -----------------------------------------------------------------------------
#weird res ratios
joined %>%
  filter(complete.cases(.)) %>%
  group_by(zip,year) %>%
  mutate(sum_res = sum(res_ratio),
         sum_tot = sum(tot_ratio),
         n = n()) %>%
  filter(sum_res < 0.99 & n>1) %>%
  arrange(zip)

## -----------------------------------------------------------------------------
#weighted sum of counties political orientation for each zip
sum_weights<-joined %>%
  group_by(zip,year) %>%
  summarise(sum_res_ratio2016 = sum(res_ratio),
            sum_tot_ratio2020 = sum(tot_ratio),
            pol2016_sum = case_when(sum(res_ratio) > 0 ~ sum(weighted_2016)/sum(res_ratio),
                                    sum(res_ratio) == 0 ~ sum(weighted_tot_2016)/sum(tot_ratio)),
            pol2020_sum = case_when(sum(res_ratio) > 0 ~ sum(weighted_2020)/sum(res_ratio),
                                    sum(res_ratio) == 0 ~ sum(weighted_tot_2020)/sum(tot_ratio)),
            res_zero = sum(res_ratio)==0,
            counties_in_zip = n()
 
  ) %>%
  mutate_at(vars(year), as.numeric)%>% ungroup()

## -----------------------------------------------------------------------------
sum_weights %>% filter(res_zero==T) %>% arrange(desc(counties_in_zip))


## -----------------------------------------------------------------------------
#merge projects to political orientation
rdf_merge <- rdf %>% mutate(join_year = ifelse(year<2018, 2016, 2020)) %>%
  left_join(sum_weights, by=c('project_zip2'='zip','join_year'='year'))


## -----------------------------------------------------------------------------
matches <- rdf_merge %>% filter(!is.na(pol2016_sum))


## -----------------------------------------------------------------------------
#no_political var
no_pol <- rdf_merge %>% filter(is.na(pol2016_sum)) 


## -----------------------------------------------------------------------------
fill_years <- sum_weights %>% filter(!is.na(pol2016_sum)) %>%
  inner_join(no_pol %>%
               distinct(project_zip2, join_year),
             by=c('zip'='project_zip2')) %>%
  mutate(diff = join_year-year) %>%
  filter(diff>0) %>%
  group_by(zip, join_year) %>%
  mutate(rank=row_number(diff)) %>%
  arrange(zip, join_year, rank) %>%
  filter(rank==1)%>%
  select(-year,-diff,-rank)%>%
  rename(year=join_year) %>%
  select(names(sum_weights))


## -----------------------------------------------------------------------------
#3824 final no voting data
fill_no_pol <- no_pol %>% select(names(rdf), join_year) %>%
  left_join(fill_years, by=c('project_zip2'='zip','join_year'='year'))


## -----------------------------------------------------------------------------
fill_no_pol %>% filter(!is.na(pol2016_sum))


## -----------------------------------------------------------------------------
pol_df <- bind_rows(matches, fill_no_pol)
pol_df %>%
  select(everything()) %>%  
  summarise_all(funs(sum(is.na(.))))


## -----------------------------------------------------------------------------
library(Hmisc)
hist.data.frame(pol_df %>% select(pol2016_sum, pol2020_sum))

## -----------------------------------------------------------------------------
pol_df <- pol_df %>% mutate(avg_pol = (pol2016_sum+pol2020_sum)/2)


## -----------------------------------------------------------------------------
hist.data.frame(pol_df %>% select(avg_pol))


## -----------------------------------------------------------------------------
#Schools data
enrollment <- readRDS('/scratch/gpfs/el1847/dec2022data/processed_data/enrollment_processed.rds')


## -----------------------------------------------------------------------------
pol_df <- pol_df %>%
  mutate(school_name = str_trim(
    str_replace_all(
      str_to_upper(project_school_name), "[^[:alnum:]]", " ")),
         project_state_name = str_trim(
           str_to_upper(project_state))) %>%
  mutate(elem_name = str_replace(
    str_replace(
      str_replace(school_name, "ELEMENTARY", "ELEM"),
      "THE", ""),
    "SCHOOL", "SCH")) %>%
  mutate(no_sch = str_replace(
    elem_name, "SCH", ""))


## -----------------------------------------------------------------------------
#897,365 projects
by_school_name <- inner_join(pol_df, 
                             enrollment, 
                             by = c("school_name"="school_name",
                                    "project_zip2"="Location.ZIP..." ,
                                    "year" = "year"), keep=F)
no_name_match <- anti_join(pol_df, 
                           enrollment, 
                           by = c("school_name"="school_name",
                                  "project_zip2"="Location.ZIP..." ,
                                  "year" = "year"))
#33,347 projects
by_school_name_year <- inner_join(no_name_match,
                                  enrollment ,
                                  by = c("school_name" = "school_name_year",
                                         "project_zip2"="Location.ZIP...",
                                         "year"="year"))
no_match <- anti_join(no_name_match,
                      enrollment ,
                      by = c("school_name" = "school_name_year",
                             "project_zip2"="Location.ZIP...",
                             "year"="year"))


## -----------------------------------------------------------------------------
no_matches <- no_match %>% sample_n(1000)
matches <- by_school_name %>% sample_n(1000)

## -----------------------------------------------------------------------------
no_matches %>% write.csv('/scratch/gpfs/el1847/dec2022data/processed_data/no_school_match_sample.csv')

## -----------------------------------------------------------------------------
enrollment <- enrollment %>%
  mutate(elem_name = str_replace(
    str_replace(
      str_replace(
        school_name,"ELEMENTARY", "ELEM"),
      "THE", ""),
    "SCHOOL", "SCH"),
         elem_name_year = str_replace(
           str_replace(
             str_replace(
               school_name_year, "ELEMENTARY", "ELEM"),
             "THE", ""),
           "SCHOOL", "SCH")    ) %>%
  mutate(no_sch = str_replace(
    elem_name, "SCH", ""),
         no_sch_year = str_replace(elem_name_year, "SCH", ""))
  


## -----------------------------------------------------------------------------
#59,199 projects
elem_match <- inner_join(no_match, enrollment ,
                               by = c("elem_name" = "elem_name",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year"), keep=F)
no_elem_match <- anti_join(no_match, enrollment ,
                               by = c("elem_name" = "elem_name",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year"))
#370 projects
elem_year_match <- inner_join(no_elem_match, enrollment,
                             by = c("elem_name" = "elem_name_year",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year")) 
no_elem_year_match <- anti_join(no_elem_match, enrollment,
                             by = c("elem_name" = "elem_name_year",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year")) 


## -----------------------------------------------------------------------------
#551 projects
sch_match <- inner_join(no_elem_year_match, enrollment ,
                               by = c("no_sch" = "no_sch",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year"))
no_sch_match <- anti_join(no_elem_year_match, enrollment ,
                               by = c("no_sch" = "no_sch",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year"))
#0 projects
sch_year_match <- inner_join(no_sch_match, enrollment,
                             by = c("no_sch" = "no_sch_year",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year")) 
#990832 total matched, 2,177,930 remaining unmatched
no_sch_year_match <- anti_join(no_sch_match, enrollment,
                             by = c("no_sch" = "no_sch_year",
                                      "project_zip2"="Location.ZIP...",
                                      "year"="year")) 

## -----------------------------------------------------------------------------
bind_rows(by_school_name,
                       by_school_name_year,
                       elem_match,
                       elem_year_match,
                       sch_match,
                       sch_year_match) %>% filter(is.na(perc_black)) %>%
  select(contains("Students"))


## -----------------------------------------------------------------------------
full_join <- bind_rows(by_school_name,
                       by_school_name_year,
                       elem_match,
                       elem_year_match,
                       sch_match,
                       sch_year_match,
                       no_sch_year_match
                       )

## -----------------------------------------------------------------------------
save_df <- full_join %>% select(-join_year,
                                  -sum_res_ratio2016,
                                  -sum_tot_ratio2020,
                                  -pol2016_sum,
                                  -pol2020_sum,
                                  -res_zero,
                                  -counties_in_zip,
                                  -elem_name,
                                  -no_sch,
                                  -School.Name,
                                  -State.Name..Latest.available.year,
                                  -State.Abbr..Latest.available.year,
                                  -School.Name...,
                                  -School.ID...NCES.Assigned..Latest.available.year,
                                  -County.Name...,
                                  -County.Number...,
                                  -ANSI.FIPS.State.Code..Latest.available.year,
                                  -Start.of.Year.Status...,
                                  -school_name_year,
                                  -state_name,
                                  -school_name.y,
                                  -school_name.x,
                                  -no_sch.x,
                                  -elem_name_year,
                                  -no_sch.y,
                                  -no_sch_year,
                                  -elem_name.y,
                                  -elem_name.x
                                  )

## -----------------------------------------------------------------------------
save_df %>% summarise_all(funs(sum(is.na(.))))

## -----------------------------------------------------------------------------
#rescale variables after all filters
save_df <- save_df %>% mutate(
        sc_bin_price = as.numeric(scale(bin_price, center=T, scale=T)),
        sc_bin_students_reached = as.numeric(scale(bin_students_reached, center=T,scale=T)),
        sc_bin_seq_n = as.numeric(scale(bin_seq_n, center=T,scale=T)),
        sc_essay_wcount = as.numeric(scale(essay_wcount, center=T, scale=T)),
        sc_fk_read = as.numeric(scale(fk_read, center=T, scale=T)),
        sc_vader = as.numeric(scale(vader, center=T, scale=T)),
        sc_referred = as.numeric(scale(referred, center=T, scale=T)),
        sc_diversity = as.numeric(scale(diversity, center=T, scale=T)),
        sc_race = as.numeric(scale(race, center=T, scale=T)),
        sc_coexist = as.numeric(scale(coexist, center=T, scale=T)),
        sc_justice = as.numeric(scale(justice, center=T, scale=T)),
        sc_represent = as.numeric(scale(represent, center=T, scale=T)),
        sc_lgbt = as.numeric(scale(lgbt, center=T, scale=T)),
        sc_disability = as.numeric(scale(disability, center=T, scale=T)),
        sc_tech = as.numeric(scale(tech, center=T, scale=T)),
        sc_sport = as.numeric(scale(sport, center=T, scale=T)))
        


## -----------------------------------------------------------------------------
save_df %>% summarise_all(mean)


## -----------------------------------------------------------------------------
saveRDS(full_join, '/scratch/gpfs/el1847/dec2022data/processed_data/r_regression_data_pol_schools_dist_modvars_2012-2022.rds')
saveRDS(save_df, '/scratch/gpfs/el1847/dec2022data/processed_data/r_moderators_regressiondf.rds')

## -----------------------------------------------------------------------------
full_join %>% summarise_all(funs(sum(is.na(.))))

