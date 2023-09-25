
library(dplyr)
library(purrr)
library(gbifmt) # my library

# setwd("C:/Users/ftw712/Desktop/doc-private-sector-data-publishing/build-table-script/")
# delete all current machine tags for privateSector.gbif.org
# pub_mt = get_mt("privateSector.gbif.org",type="organization",limit=500) %>%
# mutate(type = "organization")

# ds_mt = get_mt("privateSector.gbif.org",type="dataset",limit=500) %>%
# mutate(type="dataset")

# mt_df = list(pub_mt,ds_mt) %>% 
# bind_rows() %>% 
# glimpse()

# mt_df %>% 
# purrr::transpose() %>% 
# map(~ gbifmt::delete_mt(uuid = .x$uuid, key = .x$key, type = .x$type))

# get_mt("privateSector.gbif.org",type="organization",limit=500)
# get_mt("privateSector.gbif.org",type="dataset",limit=500)


# already tagged
pub_mt = get_mt("privateSector.gbif.org",type="organization",limit=500) %>%
mutate(type = "publisher")

pub_mt

# create new machine tags if necessary for publisher
readr::read_csv("build-table-script/data/source.tsv") %>% 
mutate(type = ifelse(grepl("publisher",link),"organization","dataset")) %>% 
filter(type == "organization") %>%  
mutate(uuid = gsub("https://www.gbif.org/publisher/","",link)) %>%
mutate(deleted = map_lgl(uuid,~ class(try(rgbif::organizations(uuid=.x))) == "try-error")) %>% 
filter(!deleted) %>% 
mutate(value = `Activity sector`) %>%
filter(!uuid %in% pub_mt$uuid) %>% # only update those that are new
glimpse() %>%
purrr::transpose() %>%
purrr::map(~ {
		  print(.x$uuid)
		  print(.x$type)
		  print(.x$value)
gbifmt::create_mt(uuid = .x$uuid,
          namespace = "privateSector.gbif.org",
          name = "privateSector",
          value = .x$value,
          type = .x$type)
		  })

# already tagged datasets 
ds_mt = get_mt("privateSector.gbif.org",type="dataset",limit=500) %>%
mutate(type="dataset")

# create new machine tags if necessary for dataset
readr::read_csv("build-table-script/data/source.tsv") %>% 
mutate(type = ifelse(grepl("publisher",link),"organization","dataset")) %>% 
filter(type == "dataset") %>%  
mutate(uuid = gsub("https://www.gbif.org/dataset/","",link)) %>%
mutate(deleted = map_lgl(uuid,~ class(try(rgbif::datasets(uuid=.x))) == "try-error")) %>% 
filter(!deleted) %>% # remove deleted 
mutate(value = `Activity sector`) %>%
filter(!uuid %in% ds_mt$uuid) %>% # only update those that are new
glimpse() %>%
purrr::transpose() %>%
purrr::map(~ {
		  print(.x$uuid)
		  print(.x$type)
		  print(.x$value)
gbifmt::create_mt(uuid = .x$uuid,
          namespace = "privateSector.gbif.org",
          name = "privateSector",
          value = .x$value,
          type = .x$type)
		  })

