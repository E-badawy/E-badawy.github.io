#-------------------------GIS Spatial Analysis in Rstudio
library(readxl)       
library(tidyverse)     
library(sf)
library(viridis)

# --- Global Malnutrition (Stunting)-----
mydata <- read_excel('dat_nutri.xlsx')
mydata$stunting <- as.numeric(mydata$stunting)

#---Import Global Shapefile
g_map <- st_read('ne_10m_admin_0_countries.shp')

map_data <- g_map %>%
  left_join(mydata, by=c('ADM0_A3' = 'ISOCode'))

#------MAP
map_data %>%
  ggplot() +
  geom_sf(aes(fill = stunting), color = "white") +
  scale_fill_gradient(
    low = "blue",   # Lower values
    high = "red",    # Higher values
    name = "Prevalence of stunting"
  ) +
  labs(
    title = "Stunting prevalence by country",
    subtitle = "UNICEF"
  ) +
  theme_minimal()



#







# setwd("P:/backup/new folder/gis/global")

map_data %>%
  ggplot()+
  geom_sf(aes(fill=stunting), color='white') +
  scale_fill_viridis_c(
    option = 'plasma', 
    direction = -1,
    name = 'Prevalence of stunting'
                       )+
  labs(
    title = 'Stunting prevalence by country',
    subtitle = 'UNICEF'
  )+
  theme_minimal()






