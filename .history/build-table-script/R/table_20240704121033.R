
library(dplyr)
library(purrr)
library(gbifmt) # my library
# setwd("C:/Users/ftw712/Desktop/doc-private-sector-data-publishing/")

# harvest publishers and datasets private sector publishers directly from machineTags

# dataset
ds_mt = get_mt("privateSector.gbif.org",type="dataset",limit=500) |> 
mutate(link = paste0("https://www.gbif.org/dataset/",uuid)) %>%
mutate(pd = "dataset") |>
select(link, `Activity sector` = value, pd, key = uuid) |>
glimpse()

# publisher
pb_mt = get_mt("privateSector.gbif.org",type="organization",limit=500) |> 
mutate(link = paste0("https://www.gbif.org/publisher/",uuid)) %>%
mutate(pd = "publisher") |>
select(link, `Activity sector` = value, pd, key = uuid) |>
glimpse()

ss = list(ds_mt,pb_mt) |> 
bind_rows() |> 
glimpse()

# don't need to read from source anymore since we read from machine tags
# ss = readr::read_csv("build-table-script/data/source.tsv") %>% 
# mutate(pd = ifelse(grepl("publisher",link),"publisher","dataset")) %>% 
# mutate(key = gsub("https://www.gbif.org/publisher/","",link)) %>%
# mutate(key = gsub("https://www.gbif.org/dataset/","",key)) %>% 
# glimpse()

gbif_country = rgbif::enumeration_country() %>% select(Country=title,iso2) %>% glimpse()

pp = ss %>%
dplyr::filter(pd == "publisher") %>%
select(key,`Activity sector`) |>
mutate(name = map_chr(key,~rgbif::organizations(uuid=.x,limit=1)$data$title)) |>
mutate(`Occurrence records` = map_dbl(key,~ rgbif::occ_search(publishingOrg = .x,occurrenceStatus=NULL,limit=0)$meta$count)) %>% 
mutate(Datasets = map_dbl(key,~rgbif::dataset_search(publishingOrg= .x,limit=0)$meta$count)) %>% 
mutate(`Data citations` = map_dbl(key,~rgbif::lit_count(publishingOrg = .x))) %>%
mutate(Company = paste0("https://www.gbif.org/publisher/",key,"[",name,"]")) %>%
mutate(iso2 = map_chr(key,~rgbif::dataset_search(publishingOrg=.x,limit=1)$data$publishingCountry)) %>%
merge(gbif_country,by="iso2") %>%
glimpse()

rgbif::dataset_get(uuid="72e23311-b65a-46d0-bc07-ff0a251b47e1")$title

dd = ss %>% 
dplyr::filter(pd == "dataset") %>%
select(key,`Activity sector`) %>% 
mutate(name = map_chr(key,~rgbif::dataset_get(uuid=.x)$title)) %>%
mutate(`Occurrence records` = map_dbl(key,~
rgbif::occ_search(datasetKey = .x,occurrenceStatus=NULL,limit=0)$meta$count)) %>%
mutate(Datasets = 1) %>% 
mutate(`Data citations` = map_dbl(key,~rgbif::lit_count(datasetKey = .x))) %>%
mutate(Company = paste0("(https://www.gbif.org/dataset/",key,")[",name,"]")) %>% 
mutate(p_key = map_chr(key,~ rgbif::dataset_get(uuid=.x)$publishingOrganizationKey)) %>%
mutate(iso2 = map_chr(p_key,~rgbif::dataset_search(publishingOrg=.x,limit=1)$data$publishingCountry)) %>%
merge(gbif_country,by="iso2") %>%
select(-p_key) %>%
glimpse()

# combine tables and clean up
tt = rbind(pp,dd) %>% 
arrange(name) %>%
select(Company, `Activity sector`,	Country, Datasets, `Occurrence records`, `Data citations`) %>%
glimpse() 

# "https://www.gbif.org/publisher/f2429cd1-4d45-475c-852a-892024cb4aba[ARC - Arctic Research and Consulting DA]",{CONSULTING},{NO},1,8 914,63

# save as csv 
tt %>%  
mutate(`Datasets` = format(`Datasets`, nsmall=0, big.mark="\u202F")) %>%
mutate(`Occurrence records` = format(`Occurrence records`, nsmall=0, big.mark="\u202F")) %>%
mutate(`Data citations` = format(`Data citations`, nsmall=0, big.mark="\u202F")) %>%
mutate(`Activity sector` = paste0("{",`Activity sector`,"}")) 

# save .adoc
# save_file_table = "250-private-sector-table.adoc"

# save and more cleanup 
# sink(file = save_file_table, type = "output")
# tt %>%  
# mutate(`Datasets` = format(`Datasets`, nsmall=0, big.mark=",")) %>%
# mutate(`Occurrence records` = format(`Occurrence records`, nsmall=0, big.mark=",")) %>%
# mutate(`Data citations` = format(`Data citations`, nsmall=0, big.mark=",")) %>%
# ascii::ascii(include.rownames = FALSE, digits = 0)
# sink()

# totals table
# save_file_totals = "260-private-sector-totals.adoc"

# sink(file = save_file_totals, type = "output")
# tt %>% 
# summarise(
# Datasets = sum(Datasets),
# `Occurrence records` = sum(`Occurrence records`), 
# `Data citations` = sum(`Data citations`)
# ) %>%
# mutate(`Datasets` = format(`Datasets`, nsmall=0, big.mark=",")) %>%
# mutate(`Occurrence records` = format(`Occurrence records`, nsmall=0, big.mark=",")) %>%
# mutate(`Data citations` = format(`Data citations`, nsmall=0, big.mark=",")) %>%
# ascii::ascii(include.rownames = FALSE, digits = 0)
# sink()
