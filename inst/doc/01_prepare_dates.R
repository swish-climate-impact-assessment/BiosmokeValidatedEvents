#################################################################
# to identify sites to be included need to know how many missing days.
# first create complete set of statoiondates for the sites per town
# this was set up after assessing the time series for completeness.  
# Perth and Launceston PM10 mindates were altered 
matrix(towns)
## [1,] "PERTH"       
## [2,] "Sydney"      
## [3,] "Illawarra"   
## [4,] "Lower Hunter"
## [5,] "Hobart"      
## [6,] "Launceston"  

# note o3 only done for towns[1:4]

for(town in towns){
#town  <- towns[2]
# housekeeping code to begin
# NB the updates made in 2015 mean that the 2007 end date is no longer
# correct in sydney
if(town == "Sydney"){
    maxdate_selected  <- as.Date('2014-12-31')
}  else {
    maxdate_selected  <- as.Date('2007-12-31')
}

  
# town=towns[4]
# for hunter make it newcastle
        if( town == "Lower Hunter"){
        town='Newcastle'
        }
# town=towns[2]
print(town)

txt <- mindates_pollutant(town = town, pollutant = "pm10_av")
mindatesp10 <- dbGetQuery(ch, txt)
mindatesp10
txt <- mindates_pollutant(town = town, pollutant = "pm25_av")
mindatesp25 <- dbGetQuery(ch, txt)
mindatesp25
txt <- mindates_pollutant(town = town, pollutant = "o3_max")
# and this one is seperate because it fails in towns without o3  
mindateo3 <- dbGetQuery(ch, txt)
mindateo3
 
# TODO it would be nice to include a user interaction stage, where the start date could be modified  
# Need to change for perth pm10 mindate because of duncraig monitoring station
if(town == "PERTH"){
mindatesp10[,2] <- as.Date('1997-05-23')
}

# in Launceston change pm10 mindate ="'1997-05-09'" changed from "'1992-05-04'" as this is start of consecutive day measurements prior to that it was weekly and seasonal
if(town == "Launceston"){
mindatesp10[,2] <- as.Date('1997-05-09')
}

#### PM10
# max date is 2007, make a table with all dates 
alldates_pm10_town  <- as.data.frame(as.Date(mindatesp10[,2]:maxdate_selected,'1970-01-01'))
alldates_pm10_town$id <- 1:nrow(alldates_pm10_town)
names(alldates_pm10_town) <- c('date','id')
dbWriteTable(ch, paste('alldates_pm10_',tolower(town),sep=''), alldates_pm10_town, row.names = F)

# make a table with every date at every station  
txt <- all_stations_all_dates(town = town, pollutant = "pm10_av")
#cat(txt)
# try to be tidy
try(
dbSendQuery(ch,paste("drop table biosmoke_pollution.stationdates_",town,"_pm10;",sep=''))
)
dbSendQuery(ch, txt)
dbSendQuery(ch,
paste('drop table alldates_pm10_',town,sep='')
)

#### PM2.5
# max date is 2007, make a table with all dates 
alldates_pm25_town  <- as.data.frame(as.Date(mindatesp25[,2]:maxdate_selected,'1970-01-01'))
alldates_pm25_town$id <- 1:nrow(alldates_pm25_town)
names(alldates_pm25_town) <- c('date','id')
dbWriteTable(ch, paste('alldates_pm25_',tolower(town),sep=''), alldates_pm25_town, row.names = F)

# make a table with every date at every station  
txt <- all_stations_all_dates(town = town, pollutant = "pm25_av")
#cat(txt)
# try to be tidy
try(
dbSendQuery(ch,paste("drop table biosmoke_pollution.stationdates_",town,"_pm25;",sep=''))
)
dbSendQuery(ch, txt)
dbSendQuery(ch,
paste('drop table alldates_pm25_',town,sep='')
)

#### O3
# max date is 2007, make a table with all dates
if(nrow(mindateo3) > 0){        
alldates_o3_town  <- as.data.frame(as.Date(mindateo3[,2]:maxdate_selected,'1970-01-01'))
alldates_o3_town$id <- 1:nrow(alldates_o3_town)
names(alldates_o3_town) <- c('date','id')
dbWriteTable(ch, paste('alldates_o3_',tolower(town),sep=''), alldates_o3_town, row.names = F)

# make a table with every date at every station  
txt <- all_stations_all_dates(town = town, pollutant = "o3_av")
#cat(txt)
# try to be tidy
try(
dbSendQuery(ch,paste("drop table biosmoke_pollution.stationdates_",town,"_o3;",sep=''))
)
dbSendQuery(ch, txt)
dbSendQuery(ch,
paste('drop table alldates_o3_',town,sep='')
)
}
        
}
