library(tidyverse)

df <- read.csv('/scratch/gpfs/el1847/dec2022data/processed_data/regressiondf_pass_r_vars_only.csv')
rdf <- df %>%
  mutate(matched = case_when(matched == "True" ~ 1,
                             matched == "False" ~ 0,
                             matched == "0" ~ 0,
                             TRUE ~ 0))%>%
 mutate(timec = interval(as.Date("2020-05-01"),mdate) %/% months(1))%>%
 mutate(across(where(is.character), ~na_if(.,""))) %>%
 filter(project_school_name!='DonorsChoose Test School') %>%
 mutate(sc_sequence_num = as.numeric(scale(project_posted_sequence_number_per_teacher, center=T, scale=T)),
        sc_students_reached = as.numeric(scale(project_students_reached, center=T, scale=T)),
        sc_essay_wcount = as.numeric(scale(essay_wcount, center=T, scale=T)),
        sc_fk_read = as.numeric(scale(fk_read, center=T, scale=T)),
        sc_vader = as.numeric(scale(vader, center=T, scale=T)),
        sc_price = as.numeric(scale(project_total_price_excluding_optional_support, center=T, scale=T)),
        sc_diversity = as.numeric(scale(diversity, center=T, scale=T)),
        sc_race = as.numeric(scale(race, center=T, scale=T)),
        sc_coexist = as.numeric(scale(coexist, center=T, scale=T)),
        sc_justice = as.numeric(scale(justice, center=T, scale=T)),
        sc_represent = as.numeric(scale(represent, center=T, scale=T)),
        sc_lgbt = as.numeric(scale(lgbt, center=T, scale=T)),
        sc_disability = as.numeric(scale(disability, center=T, scale=T)),
        sc_tech = as.numeric(scale(tech, center=T, scale=T)),
        sc_sport = as.numeric(scale(sport, center=T, scale=T)),
        sc_referred = as.numeric(scale(referred, center=T, scale=T)),
        sc_timec = timec/100,
        post = case_when(timec > 0 ~ 1,
            TRUE ~ 0))%>%
  mutate_at(vars( project_resource_category, project_grade_level, project_state,month), as.factor) %>%
  #bin variables with extreme values and skewed distributions
  mutate(bin_price = ceiling(case_when(project_total_price_excluding_optional_support > 1000 ~ 1001,
                                       T ~ project_total_price_excluding_optional_support)/10),
         bin_students_reached = ceiling(case_when(project_students_reached > 250 ~ 251,
                                                  T ~ project_students_reached)/10),
         bin_seq_n = ceiling(case_when(project_posted_sequence_number_per_teacher > 40 ~ 41,
                                       T ~ project_posted_sequence_number_per_teacher)/2)) %>%
  mutate(sc_bin_price = as.numeric(scale(bin_price, center=T, scale=T)),
         sc_bin_students_reached = as.numeric(scale(bin_students_reached, center=T,scale=T)),
         sc_bin_seq_n = as.numeric(scale(bin_seq_n, center=T,scale=T))) %>%
  filter(!is.na(project_students_reached) & !is.na(project_resource_category) & !is.na(project_grade_level))

#contrast matrices for categorical variables
c<-contr.treatment(18)
my.coding<-matrix(rep(1/18, 306), ncol=17)
my.simple<-c-my.coding
contrasts(rdf$project_resource_category)<-my.simple
joiner1 <- as_tibble(contrasts(rdf$project_resource_category),rownames="project_resource_category")%>%
  rename_at(vars(2:18),
            funs(
              paste("project_resource_category", ., sep = "_")
            )
  )
c2<-contr.treatment(4)
my.coding2<-matrix(rep(1/4, 12), ncol=3)
my.simple2<-c2-my.coding2
contrasts(rdf$project_grade_level)<-my.simple2
joiner2 <- as_tibble(contrasts(rdf$project_grade_level),rownames="project_grade_level")%>%
  rename_at(vars(2:4),
            funs(
              paste("project_grade_level", ., sep = "_")
            )
  )


c4<-contr.treatment(51)
my.coding4<-matrix(rep(1/51, 2550), ncol=50)
my.simple4<-c4-my.coding4
contrasts(rdf$project_state)<-my.simple4
joiner4 <- as_tibble(contrasts(rdf$project_state),rownames="project_state")%>%
  rename_at(vars(2:51),
            funs(
              paste("project_state", ., sep = "_")
            )
  )

c5<-contr.treatment(12)
my.coding5<-matrix(rep(1/12, 132), ncol=11)
my.simple5<-c5-my.coding5
contrasts(rdf$month)<-my.simple5
joiner5 <- as_tibble(contrasts(rdf$month),rownames="month")%>%
  rename_at(vars(2:12),
            funs(
              paste("month", ., sep = "_")
            )
  )

rdf_coded <- rdf %>% left_join(joiner1, by="project_resource_category") %>%
  left_join(joiner2, by="project_grade_level") %>%
  left_join(joiner5, by="month") %>%
  left_join(joiner4, by="project_state")
cats <- rdf_coded %>% select(project_id, matches("\\d+$"))
saveRDS(cats, file = '/scratch/gpfs/el1847/dec2022data/processed_data/categorical_coding.rds')
saveRDS(rdf_coded, file = '/scratch/gpfs/el1847/dec2022data/processed_data/r_regression_data_dropna.rds')
saveRDS(rdf, file = '/scratch/gpfs/el1847/dec2022data/processed_data/r_regression_data_dropna_uncoded.rds')