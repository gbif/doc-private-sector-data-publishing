
library(dplyr)
library(purrr)
library(gbifmt) # my library
# setwd("C:/Users/ftw712/Desktop/doc-private-sector-data-publishing/")

# harvest publishers and datasets private sector publishers directly from machineTags

# dataset machine tags
ds_mt = get_mt("privateSector.gbif.org",type="dataset",limit=500) %>% 
mutate(link = paste0("https://www.gbif.org/dataset/",uuid)) %>%
mutate(pd = "dataset") %>%
select(link, `Activity sector` = value, pd, key = uuid) %>%
glimpse()

# publisher machine tags
pb_mt = get_mt("privateSector.gbif.org",type="organization",limit=500) %>% 
mutate(link = paste0("https://www.gbif.org/publisher/",uuid)) %>%
mutate(pd = "publisher") %>%
select(link, `Activity sector` = value, pd, key = uuid) %>%
glimpse()

# combine 
ss = list(ds_mt,pb_mt) %>% 
bind_rows() %>% 
glimpse()

gbif_country = rgbif::enumeration_country() %>% select(Country=title,iso2) %>% glimpse()

pp = ss %>%
dplyr::filter(pd == "publisher") %>%
select(key,`Activity sector`) %>%
mutate(name = map_chr(key,~rgbif::organizations(uuid=.x,limit=1)$data$title)) %>%
mutate(`Occurrence records` = map_dbl(key,~ rgbif::occ_search(publishingOrg = .x,occurrenceStatus=NULL,limit=0)$meta$count)) %>% 
mutate(Datasets = map_dbl(key,~rgbif::dataset_search(publishingOrg= .x,limit=0)$meta$count)) %>% 
mutate(`Data citations` = map_dbl(key,~rgbif::lit_count(publishingOrg = .x))) %>%
mutate(Company = paste0("https://www.gbif.org/publisher/",key,"[",name,"]")) %>%
mutate(iso2 = map_chr(key,~rgbif::dataset_search(publishingOrg=.x,limit=1)$data$publishingCountry)) %>%
merge(gbif_country,by="iso2") %>%
glimpse()

dd = ss %>% 
dplyr::filter(pd == "dataset") %>%
select(key,`Activity sector`) %>% 
mutate(name = map_chr(key,~rgbif::dataset_get(uuid=.x)$title)) %>%
mutate(`Occurrence records` = map_dbl(key,~
rgbif::occ_search(datasetKey = .x,occurrenceStatus=NULL,limit=0)$meta$count)) %>%
mutate(Datasets = 1) %>% 
mutate(`Data citations` = map_dbl(key,~rgbif::lit_count(datasetKey = .x))) %>%
mutate(Company = paste0("https://www.gbif.org/dataset/",key,"[",name,"]")) %>% 
mutate(p_key = map_chr(key,~ rgbif::dataset_get(uuid=.x)$publishingOrganizationKey)) %>%
mutate(iso2 = map_chr(p_key,~rgbif::dataset_search(publishingOrg=.x,limit=1)$data$publishingCountry)) %>%
merge(gbif_country,by="iso2") %>%
select(-p_key) %>%
glimpse()

# combine tables 
tt = rbind(pp,dd) %>% 
arrange(name) %>%
select(Company, `Activity sector`,	Country = iso2, Datasets, `Occurrence records`, `Data citations`) 

# save csv and clean up  
tt %>%  
mutate(`Datasets` = trimws(format(`Datasets`, nsmall=0, big.mark="\u202F"),which ="left")) %>%
mutate(`Occurrence records` = trimws(format(`Occurrence records`, nsmall=0, big.mark="\u202F"),which ="left")) %>%
mutate(`Data citations` = trimws(format(`Data citations`, nsmall=0, big.mark="\u202F"),which ="left")) %>%
mutate(`Activity sector` = gsub(" ","_",`Activity sector`)) %>%
mutate(`Activity sector` = paste0("{",`Activity sector`,"}")) %>%
mutate(`Country` = paste0("{",`Country`,"}")) %>%
write.table(file = "250-private-sector-table.csv", row.names = FALSE, col.names = FALSE, sep = ",", quote = TRUE, qmethod = "double")

# totals table 
tt %>% 
summarise(
Datasets = sum(Datasets),
`Occurrence records` = sum(`Occurrence records`), 
`Data citations` = sum(`Data citations`)
) %>%
mutate(`Datasets` = trimws(format(`Datasets`, nsmall=0, big.mark="\u202F"),which ="left")) %>%
mutate(`Occurrence records` = trimws(format(`Occurrence records`, nsmall=0, big.mark="\u202F"),which ="left")) %>%
mutate(`Data citations` = trimws(format(`Data citations`, nsmall=0, big.mark="\u202F"),which ="left")) %>%
write.table(file = "260-private-sector-totals.csv", row.names = FALSE, col.names = FALSE, sep = ",", quote = TRUE, qmethod = "double")
