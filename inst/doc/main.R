
#################################################################
projectdir <- "~/projects/biomass_smoke_and_human_health/BiosmokeValidatedEvents"
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
poll_i <- 4
(poll <- polls[poll_i,3])
(pollutant <- polls[poll_i,2])

#### Do the processing
source("01_prepare_dates.R")

source("02_loop_over_stations_calculate_net_avg.R")
source("03_calc_extreme_events.R")
# Now Manually validate events
source("04_qc_checks.R")
