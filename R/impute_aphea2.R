
impute_aphea2 <- function(aqdf){

  library(dplyr);library(lubridate);library(tidyr)
  # making sure about date format
  aqdf$date <- dmy (aqdf$date)
  #making sure to have all the possible dates and sites
  sites <- unique(aqdf$site)
  dates <- seq (from = min(aqdf$date),
                to = max(aqdf$date),
                by = "day")
  date.site <- expand.grid(site = sites, date = dates)
  aqdf <- left_join(date.site,aqdf)
  aqdf <- aqdf %>% mutate (year = year(date))

  #average value at each site for each year
  df.year.site <- aqdf %>%
    group_by (year, site) %>%
    summarise_each (funs (mean(., na.rm = TRUE))) %>%
    select(-date) %>%
    ungroup()

  #average value for each year (across all sites)
  df.year <- aqdf %>%
    select(-site,-date) %>%
    group_by (year) %>%
    summarise_each (funs (mean(., na.rm = TRUE))) %>%
    ungroup()

  # repeating average yearly value for each site (just to calculate the ratio easily)
  years <- seq (from = min(aqdf$year),
                to = max(aqdf$year),
                by = 1)
  year.site <- expand.grid(year = years, site = sites)
  df.year <- left_join(year.site,df.year)

  # making sure that the year-site combination is exactly the same for both datasets
  df.year <- df.year %>% arrange (site, year)
  df.year.site <- df.year.site %>% arrange (site, year)

  #calculating the ratio of yearly value of each site to total
  df.ratio <- df.year.site [,3:ncol(df.year.site)] /
    df.year [,3:ncol(df.year)]
  df.ratio <- cbind (df.year.site [,1:2],df.ratio)

  # adding the dates (just repeating the ratio to cover the whole date)
  date.site.year <- date.site %>% mutate (year = year(date))
  df.ratio <- left_join(date.site.year,df.ratio)

  # calculating the average value of all sites for each day
  df.avg <- aqdf %>%
    select(-year,-site) %>%
    group_by (date) %>%
    summarise_each (funs (mean(., na.rm = TRUE))) %>%
    ungroup()

  # repeating the avg value to have it for all site and dates combination
  df.avg <- left_join(date.site.year,df.avg)

  # long formatting the airquality, average and ratio datasets
  df.aq.long <- gather (aqdf,
                            value = "concentration",
                            key = "pollutant", 3:(ncol(aqdf)-1))
  df.avg.long <- gather (df.avg,
                             value = "concentration",
                             key = "pollutant", 4:(ncol(df.avg)))
  df.ratio.long <- gather (df.ratio,
                               value = "concentration",
                               key = "pollutant", 4:(ncol(df.ratio)))

  # getting the sites, dates and pollutants with no value (NA)
  isna <- df.aq.long %>%
    filter (is.na(concentration)) %>%
    select(-concentration)

  # subsetting the avg and ratio datasets to the ones found in the previosu step
  df.avg.long.isna <- left_join(isna, df.avg.long)
  df.ratio.long.isna <- left_join(isna, df.ratio.long)

  # making sure the combinations are exactly in the same order
  df.avg.long.isna <- df.avg.long.isna %>% arrange (site,date,year)
  df.ratio.long.isna <- df.ratio.long.isna %>% arrange (site,date,year)

  # calculating the imputed value
  df.aq.imputed.isna <- data.frame(concentration =df.avg.long.isna[,5] * df.ratio.long.isna[,5])
  df.aq.imputed.isna <- cbind (df.avg.long.isna[,1:4],df.aq.imputed.isna)

  # getting the not NA values and binding them to the imputed ones
  df.aq.long.notna <- df.aq.long %>%
    filter (!is.na(concentration))
  df.aq.imputed.long <- rbind_list(df.aq.imputed.isna,df.aq.long.notna)

  # constructing the final dataframe
  df.aq.imputed <- spread(df.aq.imputed.long, key = pollutant, value = concentration)
  df.aq.imputed <- df.aq.imputed %>% select(-year)


  return(df.aq.imputed)
}
