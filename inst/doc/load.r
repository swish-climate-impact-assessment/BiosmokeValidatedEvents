
#################################################################
projectdir <- "~/data/BiosmokeValidatedEvents"
setwd(projectdir)
library(rpostgrestools)
require(R2HTML)
require(maptools)


pgpass <- get_passwordTable()
pgpass
postgis_ipaddress <- pgpass$V1[pgpass$V3 == "ewedb"]
user = "ivan_hanigan"
db = "ewedb"
pwd = readline("what is your password: ")
ch <- connect2postgres(postgis_ipaddress, db, user, pwd)

#################################################################

# list pollutants
polls <- cbind(c("sulphurdioxide_pphm","nitrogendioxide_pphm",
                 "carbonmonoxide_ppm","ozone_pphm","particulatematter10um_ugm3",
                 "nephelometer_bsp", "particulatematter2_5um_ugm3", "nitricoxide_pphm"),
               c("hrlyso2","hrlyno2","hrlyco" ,    "hrlyo3", "hrlypm10", "hrlybsp",
                 "hrlypm25",  "hrlyno"),
               c("SO2","NO2","CO","O3","PM10","BSP","PM25","NO")
               )
polls

(poll <- polls[4,3])

#################################################################
# to identify sites to be included need to know how many missing days.
# first create complete set of statoiondates for the sites per town
# this was set up after assessing the time series for completeness.  
# Perth and Launceston PM10 mindates were altered 

# note o3 only done for towns[1:4]
# then just limit to [5] and so o3 fails, then [6] and o3 fails, allgood
for(town in towns[4]){
 town=towns[4]
# for hunter make it newcastle
        if( town == "Lower Hunter"){
        town='Newcastle'
        }
# town=towns[2]
print(town)

# TODO it would be better to refactor this into a function that does the town/pollutant combo several times rather than this combined query doing same thing 3 times
mindates <- dbGetQuery(ch,
# cat(
paste('select t1.r2, min(t1.date) as minpm10,min(t2.date) as minpm25
from
(SELECT  combined_pollutants2.r2, date,avg(pm10_av) as pm10_avg
        FROM pollution.combined_pollutants 
        join 
        (
                select t1.site,t1.region as r2, t2.studysite as region
                from spatial.pollution_stations_combined_final t1
health.study_slas_01 t2
                where st_intersects(t1.gda94_geom,t2.the_geom)
                        and lower(
      case when t2.studysite like \'Sydney%\' then \'Sydney\' else t2.studysite end 
      ) = \'',tolower(town),'\'
                order by studysite
        ) combined_pollutants2 
        on
(pollution.combined_pollutants.site=combined_pollutants2.site)
        where pm10_av is not null
        group by r2,date
        order by r2, date) t1

(SELECT  combined_pollutants2.r2, date,avg(pm25_av) as pm25_avg
        FROM pollution.combined_pollutants 
        join 
        (
                select t1.site,t1.region as r2, t2.studysite as region
                from spatial.pollution_stations_combined_final t1
health.study_slas_01 t2
                where st_intersects(t1.gda94_geom,t2.the_geom)
                        and lower(
      case when t2.studysite like \'Sydney%\' then \'Sydney\' else t2.studysite end 
      ) = \'',tolower(town),'\'
                order by studysite
        ) combined_pollutants2 
        on
(pollution.combined_pollutants.site=combined_pollutants2.site)
        where pm25_av is not null 
        group by r2,date
        order by r2, date) t2
        group by t1.r2',sep='')
        )
# and this on is seperate because it fails in towns without o3  
o3mindate<- dbGetQuery(ch,
# cat(
paste('select t1.r2, min(t1.date) as mino3_max
from
(SELECT  combined_pollutants2.r2, date,avg(o3_max) as o3_max
        FROM pollution.combined_pollutants 
        join 
        (
                select t1.site,t1.region as r2, t2.studysite as region
                from spatial.pollution_stations_combined_final t1
health.study_slas_01 t2
                where st_intersects(t1.gda94_geom,t2.the_geom)
                        and lower(
      case when t2.studysite like \'Sydney%\' then \'Sydney\' else t2.studysite end 
      ) = \'',tolower(town),'\'
                order by studysite
        ) combined_pollutants2 
        on
(pollution.combined_pollutants.site=combined_pollutants2.site)
        where o3_max is not null
        group by r2,date
        order by r2, date) t1
        group by t1.r2',sep='')
        )

# TODO it would be nice to include a user interaction stage, where the start date could be modified  
# Need to change for perth pm10 mindate because of duncraig monitoring station
if( poll == 'PM10' & town == "PERTH"){
mindates[,2]=as.Date('1997-05-23')
}

# in Launceston change pm10 mindate ="'1997-05-09'" changed from "'1992-05-04'" as this is start of consecutive day measurements prior to that it was weekly and seasonal
if( poll == 'PM10' & town == "Launceston"){
mindates[,2]=as.Date('1997-05-09')
}

# max date is 2007
alldates_pm10_town  <- as.data.frame(as.Date(mindates[,2]:as.Date('2007-12-31'),'1970-01-01'))

alldates_pm10_town$id <- 1:nrow(alldates_pm10_town)
names(alldates_pm10_town) <- c('date','id')

#write.csv(alldates_pm10_town, paste('alldates_pm10_',town,'.csv',sep=''), row.names=F,quote=F)

dbWriteTable(ch, paste('alldates_pm10_',town,sep=''), alldates_pm10_town, row.names = F)

## load_newtable_to_postgres(
## paste('alldates_pm10_',town,'.csv',sep=''), schema='pollution', tablename=paste('alldates_pm10_',town,sep=''), pk=NULL, header=TRUE, printcopy=TRUE, sheetname="Sheet1", withoids=FALSE, pguser=user, db='weather',
## ip= postgis_ipaddress, source_file="STDIN",datecol='date'
## )

 

try(
dbSendQuery(ch,paste("drop table pollution.stationdates_",town,"_pm10;",sep=''))
)
dbSendQuery(ch,
#       cat(
        paste("
        select site as station, date 
        into pollution.stationdates_",town,"_pm10
        from
        (select distinct pollution.combined_pollutants.site 
        from pollution.combined_pollutants
        join
                (
                select t1.site,t2.studysite as region
                from spatial.pollution_stations_combined_final t1 , health.study_slas_01 t2
                where st_intersects(t1.gda94_geom,t2.the_geom) and upper(t2.studysite) like '",toupper(town),"%'
                order by studysite
                ) combined_pollutants2
        on pollution.combined_pollutants.site=combined_pollutants2.site
        ) sites,
        (select * from pollution.alldates_pm10_",town,") dates
        ",sep="")
        )

dbSendQuery(ch,
paste('drop table pollution.alldates_pm10_',town,sep='')
)

#file.remove(paste('alldates_pm10_',town,'.csv',sep=''))

#########################       
alldates_pm25_town=as.data.frame(as.Date(mindates[,3]:as.Date('2007-12-31'),'1970-01-01'))
alldates_pm25_town$id=1:nrow(alldates_pm25_town)
names(alldates_pm25_town)=c('date','id')
dbWriteTable(ch, paste('alldates_pm25_',town,sep=''), alldates_pm25_town, row.names = F)

#write.csv(alldates_pm25_town,paste('alldates_pm25_',town,'.csv',sep=''),row.names=F,quote=F)
#load_newtable_to_postgres(paste('alldates_pm25_',town,'.csv',sep=''),schema='pollution',tablename=paste('alldates_pm25_',town,sep=''),pk=NULL,header=TRUE,printcopy=TRUE,sheetname="Sheet1",withoids=FALSE,pguser="ivan_hanigan",db='weather',ip='",postgis_ipaddress,"',source_file="STDIN",datecol='date')

# modified to write to bio
#system(paste("type sqlquery.txt \"alldates_pm25_",town,".csv\" | \"C:\\Program Files\\PostgreSQL\\8.3\\bin\\psql\" -h ",postgis_ipaddress," -U ivan_hanigan -d bio",sep="")) 

try(
dbSendQuery(ch,
#       cat(
        paste("drop table pollution.stationdates_",town,"_pm25;",sep='')
        )
)
dbSendQuery(ch,
#       cat(
        paste("
        select site as station, date 
        into pollution.stationdates_",town,"_pm25
        from
        (select distinct pollution.combined_pollutants.site 
        from pollution.combined_pollutants
        join
                (
                select t1.site,t2.studysite as region
                from spatial.pollution_stations_combined_final t1 ,health.study_slas_01 t2
                where st_intersects(t1.gda94_geom,t2.the_geom) and upper(t2.studysite) like '",toupper(town),"%'
                order by studysite
                ) combined_pollutants2
        on pollution.combined_pollutants.site=combined_pollutants2.site
        ) sites,
        (select * from pollution.alldates_pm25_",town,") dates
        ",sep="")
        )

dbSendQuery(ch,
paste('drop table pollution.alldates_pm25_',town,sep='')
)

#file.remove(paste('alldates_pm25_',town,'.csv',sep=''))
#file.remove('sqlquery.txt')


#########################       
alldates_o3_town=as.data.frame(as.Date(o3mindate[,2]:as.Date('2007-12-31'),'1970-01-01'))
alldates_o3_town$id=1:nrow(alldates_o3_town)
names(alldates_o3_town)=c('date','id')
dbWriteTable(ch, paste('alldates_o3_',town,sep=''), alldates_o3_town, row.names = F)

#write.csv(alldates_o3_town,paste('alldates_o3_',town,'.csv',sep=''),row.names=F,quote=F)
#load_newtable_to_postgres(paste('alldates_o3_',town,'.csv',sep=''),schema='pollution',tablename=paste('alldates_o3_',town,sep=''),pk=NULL,header=TRUE,printcopy=TRUE,sheetname="Sheet1",withoids=FALSE,pguser="ivan_hanigan",db='weather',ip='",postgis_ipaddress,"',source_file="STDIN",datecol='date')


# modified to write to bio
#system(paste("type sqlquery.txt \"alldates_o3_",town,".csv\" | \"C:\\Program Files\\PostgreSQL\\8.3\\bin\\psql\" -h ",postgis_ipaddress," -U ivan_hanigan -d bio",sep="")) 


try(
dbSendQuery(ch,
#       cat(
        paste("drop table pollution.stationdates_",town,"_o3;",sep="")
        )
)
dbSendQuery(ch,
#       cat(    
        paste("select site as station, date 
        into pollution.stationdates_",town,"_o3
        from
        (select distinct pollution.combined_pollutants.site 
        from pollution.combined_pollutants
        join
                (
                select t1.site,t2.studysite as region
                from spatial.pollution_stations_combined_final t1 ,health.study_slas_01 t2
                where st_intersects(t1.gda94_geom,t2.the_geom) and upper(t2.studysite) like '",toupper(town),"%'
                order by studysite
                ) combined_pollutants2
        on pollution.combined_pollutants.site=combined_pollutants2.site
        ) sites,
        (select * from pollution.alldates_o3_",town,") dates
        ",sep="")
        )

dbSendQuery(ch,
paste('drop table pollution.alldates_o3_',town,sep='')
)

#file.remove(paste('alldates_o3_',town,'.csv',sep=''))
#file.remove('sqlquery.txt')

}




# save.image('impute.Rdata')
