
# todo.r

# to do
library(rpostgrestools)
ch <- connect2postgres2('delphe')

# perthPM10="'1997-05-23'"  (changed from 1996-06-15)
# perthPM25="'1994-02-15'"   

# sydneypm2.5="'1996-05-07'"
# sydneypm10="'1994-01-01'"

# illawarra only need to do ozone?  still needs missing days done
# illawarraPM10="'1994-02-15'"
# illawarraPM25="'1998-03-01'" 

# newcastlepm2.5="'1996-06-19'"
# Newcastle     PM10    ="'1994-02-02'"

# hobart
# hobart pm25="'2006-06-05'"
# hobart pm10= "'2006-04-22'" 

# launceston
# mindate pm25="'2005-06-04'"
# min pm10 ="'1997-05-09'" changed from "'1992-05-04'" as this is start of consecutive day measurements 
# on 2010/04/14 I changed this again to the 1/5/2001 as this was the first year they went through the summer too

towns
todo=cbind(towns,rep('pm10',length(towns)),c("'1997-05-23'","'1994-01-01'","'1994-02-15'","'1994-02-02'","'2006-04-22'" ,"'2001-05-01'"))

todo=rbind(todo,cbind(towns,rep('pm25',length(towns)),c("'1994-02-15'","'1996-05-07'","'1998-03-01'" ,"'1996-06-19'","'2006-06-05'" ,"'2005-06-04'")))

todo=rbind(todo,cbind(towns[1:4],rep('o3',4),rep("'1994-01-01'",4)))


todo=as.data.frame(todo)
todo
todo$stat=ifelse(todo[,2]=='o3','max','av')
todo
i=1
town=todo[i,1]
poll=todo[i,2]
mindate=todo[i,3]
stat=todo[i,4]

# step one get a list of the sites to do
sites_todo <- function(town,mindate,maxdate="'2007-12-31'",threshold=0.7,poll,stat){

print(poll);print(town)
print(stat)
# av or max?


# find the stations with complete
d<- dbGetQuery(ch,
# writeClipboard(
# cat(
paste("
select site,count,count(*) as potential, cast(count as numeric)/cast(count(*) as numeric) as complete
from
        (
        select polls.* , valid.count,mindate.*
        from 
        (
                (
                SELECT pollution.stationdates_",town,"_",poll,".station as site, pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
                FROM
                pollution.stationdates_",town,"_",poll,"
                left join
                pollution.combined_pollutants
                on pollution.stationdates_",town,"_",poll,".station=pollution.combined_pollutants.site
                and pollution.stationdates_",town,"_",poll,".date=pollution.combined_pollutants.date
                ) polls
        join 
                (
                SELECT pollution.stationdates_",town,"_",poll,".station as site, count(",poll,"_",stat,"), min(pollution.combined_pollutants.date)
                FROM
                pollution.stationdates_",town,"_",poll,"
                left join
                pollution.combined_pollutants
                on pollution.stationdates_",town,"_",poll,".station=pollution.combined_pollutants.site
                and pollution.stationdates_",town,"_",poll,".date=pollution.combined_pollutants.date
                where ",poll,"_",stat," is not null and pollution.stationdates_",town,"_",poll,".date >= ",mindate,"
                                        and pollution.stationdates_",town,"_",poll,".date <= ",maxdate,"
                group by pollution.stationdates_",town,"_",poll,".station
                ) valid
        on polls.site=valid.site
         
        ),
                (
                SELECT  min(pollution.combined_pollutants.date), max(pollution.combined_pollutants.date)
                FROM
                pollution.stationdates_",town,"_",poll,"
                left join
                pollution.combined_pollutants
                on pollution.stationdates_",town,"_",poll,".station=pollution.combined_pollutants.site
                and pollution.stationdates_",town,"_",poll,".date=pollution.combined_pollutants.date
                where ",poll,"_",stat," is not null
                ) mindate
        where polls.date >= ",mindate," and polls.date <= ",maxdate,"
        order by polls.date
        ) foo
group by site, count
having cast(count as numeric)/cast(count(*) as numeric) >=",threshold,"
",sep="")
)

sitelist=d$site

}
# outputs sitelist

# OK for these sites in turn.
 
                # a) calculate a daily network average of all non-missing sites (ie without the focal station of the loop)
                # b) calculate a 3-month seasonal mean for this average of all non-missing sites
                # c) calculate a 3-month seasonal mean for MISSING site
                # d) estimate missing days at missing sites

# finally join all sites for city wide averages and fill any missing days with avg of before and after                  
sitelist=sites_todo(town=town,mindate=mindate,poll=poll,stat=stat)
sitelist

impute <- function(sitelist, town, poll, stat){

# first make a table
try(dbSendQuery(ch,
# cat(
paste("drop TABLE pollution.imputed_",poll,"_",town,sep='')
),silent=T)


dbSendQuery(ch,
# cat(
paste("CREATE TABLE pollution.imputed_",poll,"_",town,"
(
  site character varying(255),
  rawdate date,
  rawdata double precision,
  date date,
  networkavg double precision,
  missingavg3mo double precision,
  networkavg3mo double precision,
  imputed double precision,
  imputed_param double precision
)",sep="")
)


for(loc in sitelist[1:length(sitelist)]){
#loc=sitelist[1]
print(loc)

# a) calculate a daily network average of all non-missing sites 
strt=Sys.time()
dbSendQuery(ch,
# cat(
paste("select date, avg(param) as networkavg         
into pollution.networkavg
from 
(",
paste("
SELECT pollution.stationdates_",town,"_",poll,".station as site, pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
FROM
pollution.stationdates_",town,"_",poll,"
left join
pollution.combined_pollutants
on pollution.stationdates_",town,"_",poll,".station=pollution.combined_pollutants.site
and pollution.stationdates_",town,"_",poll,".date=pollution.combined_pollutants.date
where pollution.stationdates_",town,"_",poll,".station = '",sitelist[-grep(loc,sitelist)],"'
                        and pollution.stationdates_",town,"_",poll,".date >= ",mindate," and pollution.stationdates_",town,"_",poll,".date <= ",maxdate,"
",sep="",collapse="union"),
") t1
where param is not null
group by date
order by date",sep="")
)
endd=Sys.time()
print(endd-strt)





# b) calculate a 3-month seasonal mean for this average of all non-missing sites

# NB -45 and + 44 after reading the SAS CMOVAVE info as this is what it does when given an even number (90)
strt=Sys.time()
dbSendQuery(ch,
"select t1.date, avg(t2.networkavg) as networkavg3mo          
into pollution.networkavg3mo
from
pollution.networkavg t1,
pollution.networkavg t2
where (t2.date >= (t1.date -45) and t2.date <= (t1.date+44))
group by t1.date 
having count(t2.networkavg)>=(90*0.75)
order by t1.date"
)
endd=Sys.time()
print(endd-strt)


# c) calculate a 3-month seasonal mean for MISSING site

strt=Sys.time()
dbSendQuery(ch,
# cat(
paste("select t1.date, avg(t2.param) as missingavg3mo       
into pollution.missingavg3mo
from 
(
SELECT pollution.stationdates_",town,"_",poll,".station as site, pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
FROM
pollution.stationdates_",town,"_",poll,"
left join
pollution.combined_pollutants
on pollution.stationdates_",town,"_",poll,".station=pollution.combined_pollutants.site
and pollution.stationdates_",town,"_",poll,".date=pollution.combined_pollutants.date
where pollution.stationdates_",town,"_",poll,".station = '",sitelist[grep(loc,sitelist)],"'
                        and pollution.stationdates_",town,"_",poll,".date >= ",mindate," and pollution.stationdates_",town,"_",poll,".date <= ",maxdate,"
) t1
(
SELECT pollution.stationdates_",town,"_",poll,".station as site, pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
FROM
pollution.stationdates_",town,"_",poll,"
left join
pollution.combined_pollutants
on pollution.stationdates_",town,"_",poll,".station=pollution.combined_pollutants.site
and pollution.stationdates_",town,"_",poll,".date=pollution.combined_pollutants.date
where pollution.stationdates_",town,"_",poll,".station = '",sitelist[grep(loc,sitelist)],"'
                        and pollution.stationdates_",town,"_",poll,".date >= ",mindate," and pollution.stationdates_",town,"_",poll,".date <= ",maxdate,"
) t2
where (t2.date >= (t1.date -45) and t2.date <= (t1.date+44))
group by t1.date 
having count(t2.param)>=(90*0.75)",sep="")
)
endd=Sys.time()
print(endd-strt)





# d) estimate missing days at missing sites and insert to output table
strt=Sys.time()
dbSendQuery(ch,
#cat(
paste("INSERT INTO  pollution.imputed_",poll,"_",town,"  (
            site, rawdate, rawdata, date, networkavg, missingavg3mo, networkavg3mo, 
            imputed, imputed_param
                                                )
select raw.site, raw.date as rawdate, param as rawdata, imputed.date, networkavg, missingavg3mo, networkavg3mo, 
            imputed, case when param is null then imputed else param end as imputed_param 
from
(
SELECT pollution.stationdates_",town,"_",poll,".station as site, pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
                FROM
                pollution.stationdates_",town,"_",poll,"
                left join
                pollution.combined_pollutants
                on pollution.stationdates_",town,"_",poll,".station=pollution.combined_pollutants.site
                and pollution.stationdates_",town,"_",poll,".date=pollution.combined_pollutants.date
                                where pollution.stationdates_",town,"_",poll,".date >= ",mindate,"
                                        and pollution.stationdates_",town,"_",poll,".date <= ",maxdate,"
                                        and pollution.stationdates_",town,"_",poll,".station = '",loc,"'
order by pollution.stationdates_",town,"_",poll,".date
) raw
left join
(
select t1.date,
        t1.networkavg,
        t2.missingavg3mo,
        t3,networkavg3mo,
        t1.networkavg*(t2.missingavg3mo/t3.networkavg3mo) as imputed
from ((pollution.networkavg t1
join
        pollution.missingavg3mo t2
        on t1.date=t2.date)
join
        pollution.networkavg3mo t3
        on t1.date=t3.date)
order by t1.date
) imputed
on raw.date=imputed.date
order by raw.date
",sep="")
                )
                
endd=Sys.time()
print(endd-strt)


dbSendQuery(ch,"drop table pollution.networkavg ;")
dbSendQuery(ch,"drop table pollution.missingavg3mo;")
dbSendQuery(ch,"drop table pollution.networkavg3mo;")

}

}

impute(sitelist, town, poll, stat)


#################################################################################
# finally avg all sites per day for city wide averages  
# AND fill any missing days with avg of before and after (if this is less than 5% of days)
# first make sure the number of missing days with one valid either side is < 5% of total days

n_missing <- function(town,poll,thresh=0.05){

nmissing<- dbGetQuery(ch,
# cat(
paste("
select count(*) from
(
select 
 t1.rawdate, avg(t2.",poll,") as citywide_",poll," , count(*)
from
        (
        select rawdate , avg(imputed_param) as ",poll,"
        from pollution.imputed_",poll,"_",town,"
        group by rawdate
        having avg(imputed_param) is null
        ) t1

        (
        select rawdate , avg(imputed_param) as ",poll,"
        from pollution.imputed_",poll,"_",town,"
        group by rawdate
        ) t2
where (t2.rawdate >= t1.rawdate-1 and  t2.rawdate <= t1.rawdate+1)
group by t1.rawdate
having count(t2.",poll,")>1
order by t1.rawdate
) foo
",sep="")
)

noverall<- dbGetQuery(ch,
paste("select count(*) from
(
select rawdate , avg(imputed_param) as ",poll,"
from pollution.imputed_",poll,"_",town,"
group by rawdate
) bar",sep="")
)

if(nmissing/noverall<=thresh){"go for it"} else {"don't do the avg of the missing dates with before and after, too many"}

}

n_missing(town,poll)

# if = 'go for it'
citywide_av <- function(town, poll, stat){

# calculate and insert to temp table
try(dbSendQuery(ch,
#cat(
paste("drop TABLE pollution.",poll,"_",stat,"_events_",town,"_temp",sep='')
),silent=T)

dbSendQuery(ch,
#cat(
paste("CREATE TABLE pollution.",poll,"_",stat,"_events_",town,"_temp
(
  date date NOT NULL,
  ",poll,"_",stat," numeric,
  ranked serial
)",sep="")
)

dbSendQuery(ch,
#cat(
paste("
INSERT INTO pollution.",poll,"_",stat,"_events_",town,"_temp (
    date, ",poll,"_",stat,")
select citywide.rawdate,
        case when citywide.",poll," is null then citywide_",poll," else ",poll," end as citywide_",poll,"
from
        (
        select rawdate , avg(imputed_param) as ",poll,"
        from pollution.imputed_",poll,"_",town,"
        group by rawdate
        ) citywide
left join
        (
        select 
                t1.rawdate, avg(t2.",poll,") as citywide_",poll," , count(*)
        from
                (
                select rawdate , avg(imputed_param) as ",poll,"
                from pollution.imputed_",poll,"_",town,"
                group by rawdate
                having avg(imputed_param) is null
                ) t1
        ,
                (
                select rawdate , avg(imputed_param) as ",poll,"
                from pollution.imputed_",poll,"_",town,"
                group by rawdate
                ) t2
        where (t2.rawdate >= t1.rawdate-1 and  t2.rawdate <= t1.rawdate+1)
        group by t1.rawdate
        having count(t2.",poll,")>1
        order by t1.rawdate
        ) impute_missing_days
on citywide.rawdate=impute_missing_days.rawdate
where case when citywide.",poll," is null then citywide_",poll," else ",poll," end is not null
order by case when citywide.",poll," is null then citywide_",poll," else ",poll," end
",sep="")
)

# ok calculate % and insert to output table
dbSendQuery(ch,
#cat(
paste("drop TABLE pollution.",poll,"_",stat,"_events_",town,sep="")
)


dbSendQuery(ch,
#cat(
paste("CREATE TABLE pollution.",poll,"_",stat,"_events_",town,"
(
  date date NOT NULL,
  ",poll,"_",stat," numeric,
  ranked numeric,
  pctile numeric
)",sep="")
)

dbSendQuery(ch,
#cat(
paste("
INSERT INTO pollution.",poll,"_",stat,"_events_",town," (
            date, ",poll,"_",stat,",ranked,pctile)
select *, (cast(ranked as numeric)-1)/(
        (
        select count(*) from pollution.",poll,"_",stat,"_events_",town,"_temp
        ) 
-1) as pctile
from pollution.",poll,"_",stat,"_events_",town,"_temp",sep="")
)
}

citywide_av(town,poll,stat)

# do all the other towns

#################################################################################################################
for(i in 2:nrow(todo)){
# i=15
town=todo[i,1]
if(town=="Lower Hunter"){
        town='Newcastle'
        } else {
        town=todo[i,1]
        }
print(town)     
poll=todo[i,2]
print(poll)
mindate=todo[i,3]
print(mindate)
stat=todo[i,4]
print(stat)

sitelist=sites_todo(town=town,mindate=mindate,poll=poll,stat=stat)

#sitelist

impute(sitelist, town, poll, stat)


nmissed=n_missing(town,poll)
print(nmissed)
if(nmissed=='go for it'){
        citywide_av(town,poll,stat)
        }
        
}


# clean up

dbSendQuery(ch,
# cat(
paste("drop table pollution.",'pm10',"_",c('av'),"_events_",gsub('Lower Hunter','Newcastle',towns),"_temp",sep='',collapse=';\n'))

dbSendQuery(ch,
# cat(
paste("drop table pollution.",'pm25',"_",c('av'),"_events_",gsub('Lower Hunter','Newcastle',towns),"_temp",sep='',collapse=';\n'))

dbSendQuery(ch,
# cat(
paste("drop table pollution.",'o3',"_",c('max'),"_events_",gsub('Lower Hunter','Newcastle',towns[1:4]),"_temp",sep='',collapse=';\n'))


# create a master table
stitch_together <- function(poll=polls[5,3]){

print(poll)

# NB only once!

exist<- dbGetQuery(ch,
#cat(
paste("select * from pollution.",poll,"_",stat,"_events_all_regions limit 1",sep='')
)

if(length(nrow(exist))==0){

        dbSendQuery(ch,
        #cat(
        paste("CREATE TABLE pollution.",poll,"_",stat,"_events_all_regions
        (
          region text,
          date date NOT NULL,
          ",poll,"_",stat," numeric,
          ranked numeric,
          pctile numeric
        )",sep="")
        )

}

rm(exist)

for(town in towns){
if(town=="Lower Hunter"){
        town='Newcastle'
        }
        
        # dbSendQuery(ch,
        # # cat(
        # paste("delete from pollution.",poll,"_",stat,"_events_all_regions where region = \'",town,"\'",sep="")
        # )

        dbSendQuery(ch,
        # cat(
        paste("insert into pollution.",poll,"_",stat,"_events_all_regions (region, date, ",poll,"_",stat,", ranked, pctile)
        select '",town,"', date, ",poll,"_",stat,", ranked, pctile
        from  pollution.",poll,"_",stat,"_events_",town,sep="")
        )

}

}

stitch_together(poll=polls[5,3])
stitch_together(poll=polls[7,3])
stitch_together(poll=polls[4,3])

# check for duplicates
# SELECT region, date,count(*)
  # FROM pollution.o3_max_events_all_regions
  # group by region,date
  # having count(*)>1

# may have crept in via the station dates process?  
 
dbSendQuery(ch,'grant all on table pollution.pm10_av_events_all_regions to grant_williamson')
 
dbSendQuery(ch,'grant all on table pollution.pm25_av_events_all_regions to grant_williamson')
 
dbSendQuery(ch,'grant all on table pollution.o3_max_events_all_regions to grant_williamson')

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
missing99 <- function(poll){
dbSendQuery(ch,
# cat(
paste("
create or replace view pollution.",poll,"_to_check
as 
select ",poll,".*, eventid,refid, eventtype, place,mindate,maxdate, field3,field5, field7
from
pollution.",poll,"_av_events_all_regions as ",poll,"
left join
(
        SELECT t1.date, t2.*
        FROM 
                pollution.",poll,"_",stat,"_events_all_regions t1
        ,
                (
                select tab1.*, 
                case when place like 'Sydney%' then 'Sydney' else place end as region,
                field3,field5, field7 from
                ivan_hanigan.tblevents tab1
                join ivan_hanigan.tblreferences tab2
                on tab1.refid=tab2.refid
                ) t2
        where t1.region=t2.region and 
                (
                t1.date=t2.mindate 
                or
                (t1.date >= t2.mindate and t1.date <= t2.maxdate)
                )
) checked
on ",poll,".date=checked.date
and ",poll,".region=checked.region 
where pctile>=.99 and mindate is null 
  ORDER BY ",poll,".region, ",poll,".pctile DESC;
grant all on pollution.",poll,"_to_check to grant_williamson
",sep="")
)

}

missing99(poll=polls[5,3])
missing99(poll=polls[7,3])
