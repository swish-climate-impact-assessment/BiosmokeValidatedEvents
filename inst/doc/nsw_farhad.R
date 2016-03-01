# NSW air quality data (received from EPA) -----
# The lines with "#" in front are for Pernilla's analysis
library (dplyr); library(openair);library(reshape2);library(stringr);library(tidyr); library(readr);library(lubridate)

#importing the data (using the openair import function in combination with tb_df)
nsw9498 <-  tbl_df (import ("OEH (1994-1998)_AllSites_1hrlyData.csv"))
nsw9903 <-  tbl_df (import ("OEH (1999-2003)__AllSites_1hrlyData.csv"))
nsw0408 <-  tbl_df (import ("OEH (2004-2008)__AllSites_1hrlyData.csv"))
nsw0913 <-  tbl_df(import ("OEH (2009-2013)__AllSites_1hrlyData.csv"))

# removing the units
names(nsw9498)[2:ncol(nsw9498)] <- word(colnames(nsw9498)[2:ncol(nsw9498)], start = 1, end = -2)
names(nsw9903)[2:ncol(nsw9903)] <- word(colnames(nsw9903)[2:ncol(nsw9903)], start = 1, end = -2)
names(nsw0408)[2:ncol(nsw0408)] <- word(colnames(nsw0408)[2:ncol(nsw0408)], start = 1, end = -2)
names(nsw0913)[2:ncol(nsw0913)] <- word(colnames(nsw0913)[2:ncol(nsw0913)], start = 1, end = -2)

# importing sites locations data
nsw.locations <- read_csv ("/Users/Farhad/Desktop/UTAS desktop/UTAS/bushfire and health project/Analysis/Airquality EPA/csv raw files/NSW/other/locations.csv")

# binding them together
nswaq0413 <- bind_rows (nsw9498,nsw9903,nsw0408, nsw0913)

# removing the unnecessary data
remove (nsw9498,nsw9903,nsw0408, nsw0913)

#lowercasing the column names
names(nswaq0413) <- tolower(colnames(nswaq0413))

# converting all columns but date to "double"
cols = seq(from=2, to=ncol(nswaq0413));    
nswaq0413[,cols] = apply(nswaq0413[,cols], 2, function(x) as.double(as.character(x)))

# removing the columns which all values are NA's
nswaq0413 <- nswaq0413[,colSums(is.na(nswaq0413))< nrow(nswaq0413)]

# keeping the variables of interest (pm2.5, pm10, o3, no, co, humidity, and temperature)
nswaq0413 <- nswaq0413 %>%
  select (date, contains ("pm2.5"), contains("pm10"), contains ("ozone"), contains ("hum"), contains ("tem"), contains ("no"), contains ("co"), contains ("so2"))

# checking the class of each column
lapply (nswaq0413, class)

# calculating the daily average (at least 75% of data needed for each day otherwise NA)
# WS and WD data should not be averaged using this approach (the columns heading needs to change to ws and wd firstly)
nswaq0413.daily.avg <- timeAverage (nswaq0413, avg.time = "day", data.thresh = 75, interval = "hour")
nswaq0413.daily.1hrmax <- timeAverage (nswaq0413, avg.time = "day", statistic = "max", data.thresh = 75, interval = "hour") %>%
  select ( contains("date"), contains ("no2"), contains ("ozone"))
colnames <- colnames (nswaq0413.daily.1hrmax)
colnames <- gsub ("no2","no2max",colnames)
colnames <- gsub ("ozone","o3max",colnames) 
colnames (nswaq0413.daily.1hrmax) <- colnames

nswaq0413.daily <- left_join (nswaq0413.daily.avg, nswaq0413.daily.1hrmax)

# making a long formatted data
nswaq0413.daily.long <- melt (nswaq0413.daily, id = "date", value.name = "concentration")

# splitting the variable to site and the type of observation
site <- word (nswaq0413.daily.long$variable,start = 1, end = 3) # get the first 3 words (some sites' names include more than one word)
site <- gsub ("1h","",site) # getting rid of unnecessary words to get down to the site name only
site <- gsub (" o3max", "", site)
site <- gsub (" no2max", "", site)
site <- gsub (" pm10","",site)
site <- gsub (" pm2.5","",site)
site <- gsub (" temp","",site)
site <- gsub (" humid", "", site)
site <- gsub (" ozone", "", site)
site <- gsub (" nox", "", site)
site <- gsub (" no2", "", site)
site <- gsub (" no", "", site)
site <- gsub (" co", "", site)
site <- gsub (" so2", "", site)

site <- str_trim(site, side = "both") #removing space from both sides of the name 
site <- gsub (" ", ".", site) #replacing space by dot

#removing space from both side 
variable <- str_trim(nswaq0413.daily.long$variable, side = "both") 
variable <- word(variable, -3) # getting the 3rd word from the end (it is what we want (the name of the variable))
variables <- c ("humid" = "humidity", "ozone" = "o3", "pm10" ="pm10", "pm2.5" = "pm2.5", "temp" = "temp", "co" = "co", "no" = "no", "no2" = "no2", "nox" = "nox", "so2" = "so2", "no2max" = "no2max", "o3max" = "o3max")

# adding the "observation" and "site" column to our data
nswaq0413.daily.long$observation <- factor (variables[variable], levels = variables)
nswaq0413.daily.long$site <- site

# removing unnecessary data
remove (site,variable,variables)

# building the tidy data (each column a variable)
nswaq0413.daily <- 
  tbl_df(nswaq0413.daily.long) %>%
  select (-variable) %>%
  filter (!is.na(observation)) %>%
  spread (observation, concentration) %>%
  arrange (site)

# removing unnecessary data
remove (nswaq0413.daily.long)

# attaching the sites locations (lon and lat)
nswaq0413.daily <- left_join (nswaq0413.daily, nsw.locations)

# remove unnecessary data
remove (nsw.locations,nswaq0413.daily.avg,nswaq0413.daily.1hrmax)

# change date format to Date
nswaq0413.daily <- nswaq0413.daily %>% mutate (date = as.Date(date))

#subsetting to sydney stations
nswaq0413.daily.sydney <- nswaq0413.daily %>%
  filter ( site %in% c ("bringelly", "camden", "chullora", "campbelltown.west","earlwood","lindfield","liverpool","oakdale","prospect","randwick","richmond","rozelle","st.marys","vineyard")) 

# selecting the sites which have at least 75% of data available for each variable
sites <- nswaq0413.daily.sydney %>%
  group_by (site) %>%
  summarise (total.count = n(), na.pm2.5 = sum(is.na(pm2.5)), na.pm10 = sum(is.na(pm10)), na.humidity = sum(is.na(humidity)), na.o3 = sum(is.na(o3)), na.o3max = sum(is.na(o3max)), na.temp = sum(is.na(temp)), na.co = sum(is.na(co)), na.no = sum(is.na(no)), na.no2 = sum(is.na(no2)), na.nox = sum(is.na(nox)), na.so2 = sum(is.na(so2))) %>%
  mutate (pm2.5.na.percent = na.pm2.5/total.count, pm10.na.percent = na.pm10/total.count, humidity.na.percent = na.humidity/total.count, o3.na.percent = na.o3/total.count,o3max.na.percent = na.o3max/total.count, temp.na.percent = na.temp/total.count, co.na.percent = na.co/total.count, no.na.percent = na.co/total.count, no2.na.percent = na.no2/total.count, nox.na.percent = na.nox/total.count, so2.na.percent = na.so2/total.count)

pm2.5.sites <- sites %>% filter (pm2.5.na.percent <= 0.25) %>% select (site)
pm10.sites <- sites %>% filter (pm10.na.percent <= 0.25) %>% select (site)  
o3.sites <- sites %>% filter (o3.na.percent <= 0.25) %>% select (site)
o3max.sites <- sites %>% filter (o3max.na.percent <= 0.25) %>% select (site)
humidity.sites <- sites %>% filter (humidity.na.percent <= 0.25) %>% select (site)
temp.sites <- sites %>% filter (temp.na.percent <= 0.25) %>% select (site)
co.sites <- sites %>% filter (co.na.percent <= 0.25) %>% select (site)
no.sites <- sites %>% filter (no.na.percent <= 0.25) %>% select (site)
no2.sites <- sites %>% filter (no2.na.percent <= 0.25) %>% select (site)
nox.sites <- sites %>% filter (nox.na.percent <= 0.25) %>% select (site)
so2.sites <- sites %>% filter (so2.na.percent <= 0.25) %>% select (site)

nswaq0413.pm2.5.daily.sydney <- left_join (pm2.5.sites, nswaq0413.daily.sydney)
nswaq0413.pm10.daily.sydney <- left_join (pm10.sites, nswaq0413.daily.sydney)
nswaq0413.o3.daily.sydney <- left_join (o3.sites, nswaq0413.daily.sydney)
nswaq0413.o3max.daily.sydney <- left_join (o3max.sites, nswaq0413.daily.sydney)
nswaq0413.humidity.daily.sydney <- left_join (humidity.sites, nswaq0413.daily.sydney)
nswaq0413.temp.daily.sydney <- left_join (temp.sites, nswaq0413.daily.sydney)
nswaq0413.co.daily.sydney <- left_join (co.sites, nswaq0413.daily.sydney)
nswaq0413.no.daily.sydney <- left_join (no.sites, nswaq0413.daily.sydney)
nswaq0413.no2.daily.sydney <- left_join (no2.sites, nswaq0413.daily.sydney)
nswaq0413.nox.daily.sydney <- left_join (nox.sites, nswaq0413.daily.sydney)
nswaq0413.so2.daily.sydney <- left_join (so2.sites, nswaq0413.daily.sydney)

#pm2.5 imputation
data <- nswaq0413.pm2.5.daily.sydney %>% select (date, site, pm2.5)
data <- data %>% mutate (month = month(date), year = year(date)) %>%
  mutate(season = ifelse (month == 12 | month ==1 | month == 2, "summer",.) %>%
           ifelse (month == 3 | month == 4 | month == 5,"autumn",.) %>%
           ifelse(month == 6 | month ==7 | month == 8, "winter",.) %>%
           ifelse(month == 9 | month ==10 | month == 11, "spring",.)) %>%
  mutate (season = as.character(season))

data.siteaverage <- data %>% group_by(site,year,season) %>% summarise(site.mean.pm2.5 = mean(pm2.5, na.rm =TRUE))

data.othersitesaverage3 <- data %>% filter (site != "liverpool") %>% group_by(year,season) %>% summarise(othersites.mean.pm2.5 = mean(pm2.5, na.rm =TRUE)) %>% mutate(site="liverpool")
data.othersitesaverage4 <- data %>% filter (site != "richmond") %>% group_by(year,season) %>% summarise(othersites.mean.pm2.5 = mean(pm2.5, na.rm =TRUE)) %>% mutate(site="richmond")

data.othersitesaverage <-  rbind_list(data.othersitesaverage3,data.othersitesaverage4)

data.siteandotheraverage <- full_join(data.siteaverage,data.othersitesaverage)
data.siteandotheraverage <- data.siteandotheraverage %>% mutate (factor = site.mean.pm2.5/othersites.mean.pm2.5)

data.dailyaverage <- data %>% group_by (date) %>% summarise (mean.pm2.5 = mean(pm2.5, na.rm = TRUE)) %>%
  mutate (month = month(date), year = year(date)) %>% 
  mutate(season = ifelse (month == 12 | month ==1 | month == 2, "summer",.) %>%
           ifelse (month == 3 | month == 4 | month == 5,"autumn",.) %>%
           ifelse(month == 6 | month ==7 | month == 8, "winter",.) %>%
           ifelse(month == 9 | month ==10 | month == 11, "spring",.)) %>%
  mutate (season = as.character(season))
data.impute <- left_join (data.dailyaverage,data.siteandotheraverage)
data.impute <- data.impute %>% mutate (pm2.5.impute = mean.pm2.5 * factor) %>% select (date,site,pm2.5.impute)
data.new <- left_join (data, data.impute)
data.new1 <- data.new %>% filter (is.na(pm2.5)) %>% mutate (pm2.5 = pm2.5.impute)
data.new <- data.new %>% filter (!is.na(pm2.5))
data.new <- rbind_list(data.new, data.new1)
data.new <-  data.new %>% select(-c(pm2.5.impute, season, year, month))

nswaq0413.pm2.5.daily.sydney  <- nswaq0413.pm2.5.daily.sydney %>% select(-pm2.5)
nswaq0413.pm2.5.daily.sydney <- left_join(nswaq0413.pm2.5.daily.sydney, data.new)

remove (data.othersitesaverage3,data.othersitesaverage4,data.siteaverage,data.othersitesaverage,data.siteandotheraverage,data.dailyaverage,data.impute,data.new,data.new1)

nswaq0413.pm2.5.daily.sydney <- nswaq0413.pm2.5.daily.sydney %>% group_by (date) %>% summarise (pm2.5 = mean (pm2.5, na.rm = TRUE))
nswaq0413.pm10.daily.sydney <- nswaq0413.pm10.daily.sydney %>% group_by (date) %>% summarise (pm10 = mean (pm10, na.rm = TRUE))
nswaq0413.o3.daily.sydney <- nswaq0413.o3.daily.sydney %>% group_by (date) %>% summarise (o3 = mean (o3, na.rm = TRUE))
nswaq0413.o3max.daily.sydney <- nswaq0413.o3max.daily.sydney %>% group_by (date) %>% summarise (o3max = mean (o3max, na.rm = TRUE))
nswaq0413.humidity.daily.sydney <- nswaq0413.humidity.daily.sydney %>% group_by (date) %>% summarise (humidity = mean (humidity, na.rm = TRUE))
nswaq0413.temp.daily.sydney <- nswaq0413.temp.daily.sydney %>% group_by (date) %>% summarise (temp = mean (temp, na.rm = TRUE))
nswaq0413.co.daily.sydney <- nswaq0413.co.daily.sydney %>% group_by (date) %>% summarise (co = mean (co, na.rm = TRUE))
nswaq0413.no.daily.sydney <- nswaq0413.no.daily.sydney %>% group_by (date) %>% summarise (no = mean (no, na.rm = TRUE))
nswaq0413.no2.daily.sydney <- nswaq0413.no2.daily.sydney %>% group_by (date) %>% summarise (no2 = mean (no2, na.rm = TRUE))
nswaq0413.nox.daily.sydney <- nswaq0413.nox.daily.sydney %>% group_by (date) %>% summarise (nox = mean (nox, na.rm = TRUE))
nswaq0413.so2.daily.sydney <- nswaq0413.so2.daily.sydney %>% group_by (date) %>% summarise (so2 = mean (so2, na.rm = TRUE))

nswaq0413.daily.sydney <- left_join (nswaq0413.pm2.5.daily.sydney, nswaq0413.pm10.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.o3.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.o3max.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.humidity.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.temp.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.co.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.no.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.no2.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.nox.daily.sydney)
nswaq0413.daily.sydney <- left_join (nswaq0413.daily.sydney, nswaq0413.so2.daily.sydney)

# remove unnecessary data
remove (temp.sites, sites, so2.sites, pm10.sites, pm2.5.sites, o3.sites,humidity.sites, nswaq0413.humidity.daily.sydney, nswaq0413.o3.daily.sydney,nswaq0413.o3max.daily.sydney, nswaq0413.pm10.daily.sydney,nswaq0413.so2.daily.sydney, nswaq0413.pm2.5.daily.sydney, nswaq0413.temp.daily.sydney,nswaq0413.co.daily.sydney, nswaq0413.no.daily.sydney, nswaq0413.no2.daily.sydney, nswaq0413.nox.daily.sydney , co.sites, no.sites, no2.sites, nox.sites)

# rounding to one decimal point
nswaq0413.daily.sydney <- nswaq0413.daily.sydney %>% mutate (pm2.5=round (pm2.5, digits=1),pm10=round (pm10, digits=1),o3=round (o3, digits=1),o3max=round (o3max, digits=1),humidity=round (humidity, digits=1),temp=round (temp, digits=1),co=round (co, digits=1),no=round (no, digits=1), no2=round (no2, digits=1), nox =round (nox, digits=1), so2 =round (so2, digits=1))

# saving the file
write_csv (nswaq0413.daily.sydney, path ="nswaq9413.daily.sydney.csv")