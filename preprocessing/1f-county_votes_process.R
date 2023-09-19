library(tidyverse)

df2 <- read.csv('/scratch/gpfs/el1847/dec2022data/datasets/countypres_2000-2020.csv')

#counties in 2020 no total votes, only different modes
no_totals <- df2 %>% filter(year==2020) %>%
  pivot_wider(names_from = mode,
              values_from = c(totalvotes,candidatevotes))%>%
  filter(is.na(totalvotes_TOTAL)) %>%
  distinct(county_fips)
#sum votes for counties with no totals
sum_missing_totals <- df2 %>%
  filter(year==2020)%>%
  inner_join(no_totals) %>%
  group_by(year,
           state,
           state_po,
           county_name,
           county_fips,
           office,
           candidate,
           party,
           totalvotes,
           version) %>%
  summarise(candidatevotes = sum(candidatevotes),
            mode = 'TOTAL') %>% ungroup() %>%
  dplyr::select(names(df2)) %>%
  filter(party=='DEMOCRAT' | party=='REPUBLICAN')
#standardize county names
county_names <- df2 %>%
  group_by(county_fips) %>%
  summarise(county_name = first(county_name)) %>%
  ungroup()
#all filters, year, total, parties
df3 <- df2 %>%
  filter(year==2020 | year ==2016) %>%
  filter(mode=='TOTAL') %>%
  filter(party == 'DEMOCRAT' | party == 'REPUBLICAN') %>%
  #fill in fips for DC
  mutate(county_fips = case_when(county_name == 'DISTRICT OF COLUMBIA' ~ 11001,
                                 TRUE~as.numeric(county_fips))) %>%
  #append missing totals 
  bind_rows(., sum_missing_totals)%>%
  #replace names with standardized names
  dplyr::select(-county_name)%>%
  left_join(county_names)%>%
  #calculate vote percentage
  mutate(perc = candidatevotes/totalvotes) %>%
  #pivot to one row per county fips
  pivot_wider(names_from = party,
              values_from = c(perc, candidatevotes,candidate)) %>%
  pivot_wider(names_from = year,
              values_from = c(totalvotes,                         
                              perc_DEMOCRAT,
                              perc_REPUBLICAN,
                              candidatevotes_DEMOCRAT,
                              candidatevotes_REPUBLICAN,
                              candidate_DEMOCRAT,
                              candidate_REPUBLICAN)) %>%
  #political variable percent republican minus percent democrat
  mutate(pol2016_var = perc_REPUBLICAN_2016 - perc_DEMOCRAT_2016,
         pol2020_var = perc_REPUBLICAN_2020 - perc_DEMOCRAT_2020) %>%
  mutate(county_fips2 = stringr::str_pad(county_fips, side='left', width=5, pad='0'))

missings <- df3  %>% filter_all(any_vars(is.na(.))) 
miss_checks<-df2 %>% filter(county_fips %in% missings$county_fips & year==2020)


saveRDS(df3, '/scratch/gpfs/el1847/dec2022data/datasets/county_pres_processed.rds')

