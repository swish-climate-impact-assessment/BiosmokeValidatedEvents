#################################################################
projectdir <- "~/projects/biomass_smoke_and_human_health/BiosmokeValidatedEvents/inst/doc"
setwd(projectdir)
library(rpostgrestools)
# you will need to request username and password
ch <- connect2postgres2("ewedb_staging")
#################################################################
towns <- c("PERTH", "Sydney","Illawarra","Lower Hunter","Hobart","Launceston")
#################################################################

# list pollutants
polls <- cbind(c("sulphurdioxide_pphm","nitrogendioxide_pphm",
                 "carbonmonoxide_ppm","ozone_pphm","particulatematter10um_ugm3",
                 "nephelometer_bsp", "particulatematter2_5um_ugm3", "nitricoxide_pphm"),
               c("so2_max","no2_max","co_max" ,    "o3_max", "pm10_av", "bsp_max",
                 "pm25_av",  "no_max"),
               c("SO2","NO2","CO","O3","PM10","BSP","PM25","NO")
               )
polls
# select on for this run
poll_i <- 7
(poll <- polls[poll_i,3])
(pollutant <- polls[poll_i,2])
#### Do the processing
source("01_prepare_dates.R")
#### Set up a list of things to do in order ####
todo=cbind(towns,rep('pm10',length(towns)),c("'1997-05-23'","'1994-01-01'","'1994-02-15'",
"'1994-02-02'","'2006-04-22'" ,"'2001-05-01'"))

todo=rbind(todo,cbind(towns,rep('pm25',length(towns)),c("'1994-02-15'","'1996-05-07'","'1998-03-01'" ,"'1996-06-19'","'2006-06-05'" ,"'2005-06-04'")))

todo=rbind(todo,cbind(towns[1:4],rep('o3',4),rep("'1994-01-01'",4)))

todo=as.data.frame(todo)
todo
todo$stat=ifelse(todo[,2]=='o3','max','av')
todo

i=8
todo[i,]
town=todo[i,1]
poll=todo[i,2]
mindate="'2003-01-01'"
  #todo[i,3]
stat=todo[i,4]
maxdate_selected  <- "2014-12-31"
source("02_loop_over_stations_calculate_net_avg.R")
source("03_calc_extreme_events.R")
# Now Manually validate events
source("04_qc_checks.R")
source("05_clean_up_intermediary_tables.R")
