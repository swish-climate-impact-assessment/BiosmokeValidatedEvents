
# check for duplicates
# SELECT region, date,count(*)
  # FROM pollution.o3_max_events_all_regions
  # group by region,date
  # having count(*)>1

# may have crept in via the station dates process?  
 
dbSendQuery(ch,'grant all on table pollution.pm10_av_events_all_regions to biosmoke_group')
 
dbSendQuery(ch,'grant all on table pollution.pm25_av_events_all_regions to biosmoke_group')
 
dbSendQuery(ch,'grant all on table pollution.o3_max_events_all_regions to biosmoke_group')

############################################################# 
# summarise  

# TODO: this needs to be looped thru todo rows so the mindate can be selected and missing days counted?

descstats=data.frame(matrix(nrow=0,ncol=15))
descstats
for(i in 1:nrow(todo)){
# i=1
town=todo[i,1]
if(town=="Lower Hunter"){
        town='Newcastle'
        } else {
        town=todo[i,1]
        }
print(town)     
poll=todo[i,2]
print(poll)

if(town=="PERTH" & poll=='pm25'){
mindate=as.factor("'1994-03-01'")
        } else {
mindate=todo[i,3]
        }



print(mindate)
stat=todo[i,4]
print(stat)

# town=towns[1]
# print(town)   
        # dbSendQuery(ch,
        # # cat(
        # paste("delete from pollution.",poll,"_",stat,"_events_all_regions where region = \'",town,"\'",sep="")
        # )

d<- dbGetQuery(ch,
        # cat(
        paste("select t1.date as fulldate, t2.*
        from  
        (select distinct date from pollution.stationdates_",town,"_",poll," where date >= ",mindate,") t1 
        left join 
        (select * from pollution.",poll,"_",stat,"_events_all_regions where region =\'",town,"\') as t2
        on t1.date=t2.date",sep="")
        )
        
counts<- dbGetQuery(ch,
# cat(
paste("select \'99\', count(*)
from
(
SELECT region, date, ",poll,"_",stat,", ranked, pctile
  FROM pollution.",poll,"_",stat,"_events_all_regions
  where region = \'",town,"\' and pctile >= .99
  ) foo
union all
select \'97-98\', count(*)
from
(
SELECT region, date, ",poll,"_",stat,", ranked, pctile
  FROM pollution.",poll,"_",stat,"_events_all_regions
  where region = \'",town,"\'  and (pctile >= .97 and pctile < .99)
  ) foo
union all
select \'95-96\', count(*)
from
(
SELECT region, date, ",poll,"_",stat,", ranked, pctile
  FROM pollution.",poll,"_",stat,"_events_all_regions
  where region = \'",town,"\'  and (pctile >= .95 and pctile < .97)
  ) foo
union all
select \'95+\', count(*)
from
(
SELECT region, date, ",poll,"_",stat,", ranked, pctile
  FROM pollution.",poll,"_",stat,"_events_all_regions
  where region = \'",town,"\' and pctile >= .95
  ) foo;",sep="")
)
        
head(d)
descstats=rbind(descstats,
data.frame(t(c(as.character(town),
        paste(poll,stat),
        nrow(d),
        as.character(min(d$fulldate)),
        as.character(max(d$fulldate)),
        quantile(d[,4],.99,na.rm=T),
        quantile(d[,4],.97,na.rm=T),
        quantile(d[,4],.95,na.rm=T),
        counts[1,2],
        counts[2,2],
        counts[3,2],
        counts[4,2],
        t(
        if (length(names(summary(d[,4])))==6) {
        c(summary(d[,4]),NA)
        } else {
        summary(d[,4])
        }
        ))))
)


}

names(descstats)=c('town','poll','numDays','mindate','maxdate','99','97','95','N99','N97_98','N95_96','N95',names(summary(d[,4])))
descstats
write.csv(descstats,'descstats.csv',row.names=F)



# I did some manual validation against the original files
#M:\Environmental_Health\Bushfires\Exposures\TAS
# etc
# checked mindates, poll values, even if the single missing days were filled with av of prior and next.
# for each in todo list.
# all looks good.
# only issue was perth mindate for pm2.5 which was no longer cavershamB 15/2/94 but now cavA 1/3/94
  
# so this caveat is embedded in a if else in the descriptive stats above  


#########################################################################################################
# not changed is the underlying calculation of the percentiles as this would produce trivial changes to the percentile levels.
######################################################################################################### 

######################################################################################################### 
# NB I did not double check the OZONE values.

# useful code
# select t1.date as fulldate, t2.*
# from  
# (select distinct date from pollution.stationdates_Sydney_pm10 where date >= '1994-01-10') t1 
# left join 
# (select * from pollution.pm10_av_events_all_regions where region ='Newcastle') as t2
# on t1.date=t2.date


# select *  
# from  
# (select distinct date from pollution.stationdates_illawarra_pm25 where date = '1998-03-01') t1 
# left join 
# (
# select pollution.combined_pollutants.* 
# from pollution.combined_pollutants 
# join 
# spatial.pollution_stations_combined_final
# on
# pollution.combined_pollutants.site=spatial.pollution_stations_combined_final.site 
# where region = 'Illawara'
# ) t2
# on t1.date=t2.date

# identify 99% centile days with no refs.
missing99(poll=polls[5,3])
missing99(poll=polls[7,3])
