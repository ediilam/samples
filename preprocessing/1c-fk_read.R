
library(multidplyr)
library(quanteda)
library(quanteda.textstats)
library(dplyr)
library(readr)
library(tidyr)
library(stringr)

cluster <- new_cluster(4)
cluster_library(cluster, 'stringr')
cluster_library(cluster, 'dplyr')
cluster_library(cluster, 'quanteda')
cluster_library(cluster, 'quanteda.textstats')

projects <- read.csv('/scratch/gpfs/el1847/dec2022data/processed_data/filtered_essay_texts.csv')
partproj <- projects %>%
  group_by(year,month) %>%
  partition(cluster)
projects <- partproj %>%
  mutate(fk_read = textstat_readability(project_essay_text, measure="Flesch.Kincaid")$Flesch.Kincaid,
         essay_sentcount = nsentence(project_essay_text)) %>%
  collect()%>%
  ungroup()
write.csv(projects,'/scratch/gpfs/el1847/dec2022data/processed_data/fk_read.csv')
