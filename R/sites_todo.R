
#' @name sites_todo
#' @title sites with potential
#' @param town
#' @param mindate
#' @param maxdate
#' @param threshold
#' @param poll
#' @param stat
#' @return text for a sql query

sites_todo <- function(town, mindate, maxdate="'2007-12-31'", threshold=0.7, poll, stat){

print(poll);print(town)
print(stat)
# av or max?

# find the stations with complete
txt <- paste("
select site,count,count(*) as potential, cast(count as numeric)/cast(count(*) as numeric) as complete
from
        (
        select polls.* , valid.count,mindate.*
        from 
        (
                (
                SELECT biosmoke_pollution.stationdates_",town,"_",poll,".station as site, biosmoke_pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
                FROM
                biosmoke_pollution.stationdates_",town,"_",poll,"
                left join
                biosmoke_pollution.combined_pollutants
                on biosmoke_pollution.stationdates_",town,"_",poll,".station=biosmoke_pollution.combined_pollutants.site
                and biosmoke_pollution.stationdates_",town,"_",poll,".date=biosmoke_pollution.combined_pollutants.date
                ) polls
        join 
                (
                SELECT biosmoke_pollution.stationdates_",town,"_",poll,".station as site, count(",poll,"_",stat,"), min(biosmoke_pollution.combined_pollutants.date)
                FROM
                biosmoke_pollution.stationdates_",town,"_",poll,"
                left join
                biosmoke_pollution.combined_pollutants
                on biosmoke_pollution.stationdates_",town,"_",poll,".station=biosmoke_pollution.combined_pollutants.site
                and biosmoke_pollution.stationdates_",town,"_",poll,".date=biosmoke_pollution.combined_pollutants.date
                where ",poll,"_",stat," is not null and biosmoke_pollution.stationdates_",town,"_",poll,".date >= ",mindate,"
                                        and biosmoke_pollution.stationdates_",town,"_",poll,".date <= ",maxdate,"
                group by biosmoke_pollution.stationdates_",town,"_",poll,".station
                ) valid
        on polls.site=valid.site
         
        ),
                (
                SELECT  min(biosmoke_pollution.combined_pollutants.date), max(biosmoke_pollution.combined_pollutants.date)
                FROM
                biosmoke_pollution.stationdates_",town,"_",poll,"
                left join
                biosmoke_pollution.combined_pollutants
                on biosmoke_pollution.stationdates_",town,"_",poll,".station=biosmoke_pollution.combined_pollutants.site
                and biosmoke_pollution.stationdates_",town,"_",poll,".date=biosmoke_pollution.combined_pollutants.date
                where ",poll,"_",stat," is not null
                ) mindate
        where polls.date >= ",mindate," and polls.date <= ",maxdate,"
        order by polls.date
        ) foo
group by site, count
having cast(count as numeric)/cast(count(*) as numeric) >=",threshold,"
",sep="")

# cat(txt)
#d<- dbGetQuery(ch, txt)
#sitelist <- d$site
return(txt)
}

#' @name impute
#' @title impute for each site
#' @param sitelist sites
#' @param town town
#' @param poll pollutant
#' @param stat statistical unit as per avg or max
#' @param maxdate the end of the time series
#' @return database table

impute <- function(
  sitelist = c( "SouthLake", "Duncraig" )
  ,
  town = "PERTH"
  ,
  poll = "pm10"
  ,
  stat = "av"
  ,
  maxdate = "2007-12-31"
  ){

# first make a table
try(dbSendQuery(ch,
# cat(
paste("drop TABLE biosmoke_pollution.imputed_",poll,"_",town,sep='')
),silent=T)


dbSendQuery(ch,
# cat(
paste("CREATE TABLE biosmoke_pollution.imputed_",poll,"_",town,"
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
txt <- paste("select date, avg(param) as networkavg         
into biosmoke_pollution.networkavg
from 
(",
paste("
SELECT biosmoke_pollution.stationdates_",town,"_",poll,".station as site, biosmoke_pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
FROM
biosmoke_pollution.stationdates_",town,"_",poll,"
left join
biosmoke_pollution.combined_pollutants
on biosmoke_pollution.stationdates_",town,"_",poll,".station=biosmoke_pollution.combined_pollutants.site
and biosmoke_pollution.stationdates_",town,"_",poll,".date=biosmoke_pollution.combined_pollutants.date
where biosmoke_pollution.stationdates_",town,"_",poll,".station = '",sitelist[-grep(loc,sitelist)],"'
                        and biosmoke_pollution.stationdates_",town,"_",poll,".date >= ",mindate," and biosmoke_pollution.stationdates_",town,"_",poll,".date <= '",maxdate,"'
",sep="",collapse="union"),
") t1
where param is not null
group by date
order by date",sep="")

#cat(txt)

#strt=Sys.time()
dbSendQuery(ch,txt)
#endd=Sys.time()
#print(endd-strt)

# b) calculate a 3-month seasonal mean for this average of all non-missing sites

# NB -45 and + 44 after reading the SAS CMOVAVE info as this is what it does when given an even number (90)
txt <- "select t1.date, avg(t2.networkavg) as networkavg3mo          
into biosmoke_pollution.networkavg3mo
from
biosmoke_pollution.networkavg t1,
biosmoke_pollution.networkavg t2
where (t2.date >= (t1.date -45) and t2.date <= (t1.date+44))
group by t1.date 
having count(t2.networkavg)>=(90*0.75)
order by t1.date"

#strt=Sys.time()
dbSendQuery(ch,txt)
#endd=Sys.time()
#print(endd-strt)


# c) calculate a 3-month seasonal mean for MISSING site

txt <- paste("select t1.date, avg(t2.param) as missingavg3mo       
into biosmoke_pollution.missingavg3mo
from 
(
SELECT biosmoke_pollution.stationdates_",town,"_",poll,".station as site, biosmoke_pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
FROM
biosmoke_pollution.stationdates_",town,"_",poll,"
left join
biosmoke_pollution.combined_pollutants
on biosmoke_pollution.stationdates_",town,"_",poll,".station=biosmoke_pollution.combined_pollutants.site
and biosmoke_pollution.stationdates_",town,"_",poll,".date=biosmoke_pollution.combined_pollutants.date
where biosmoke_pollution.stationdates_",town,"_",poll,".station = '",sitelist[grep(loc,sitelist)],"'
                        and biosmoke_pollution.stationdates_",town,"_",poll,".date >= ",mindate," and biosmoke_pollution.stationdates_",town,"_",poll,".date <= '",maxdate,"'
) t1
(
SELECT biosmoke_pollution.stationdates_",town,"_",poll,".station as site, biosmoke_pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
FROM
biosmoke_pollution.stationdates_",town,"_",poll,"
left join
biosmoke_pollution.combined_pollutants
on biosmoke_pollution.stationdates_",town,"_",poll,".station=biosmoke_pollution.combined_pollutants.site
and biosmoke_pollution.stationdates_",town,"_",poll,".date=biosmoke_pollution.combined_pollutants.date
where biosmoke_pollution.stationdates_",town,"_",poll,".station = '",sitelist[grep(loc,sitelist)],"'
                        and biosmoke_pollution.stationdates_",town,"_",poll,".date >= ",mindate," and biosmoke_pollution.stationdates_",town,"_",poll,".date <= '",maxdate,"'
) t2
where (t2.date >= (t1.date -45) and t2.date <= (t1.date+44))
group by t1.date 
having count(t2.param)>=(90*0.75)",sep="")

# cat(txt)
strt=Sys.time()
dbSendQuery(ch,txt)
endd=Sys.time()
print(endd-strt)

# d) estimate missing days at missing sites and insert to output table
txt <- paste("INSERT INTO  biosmoke_pollution.imputed_",poll,"_",town,"  (
            site, rawdate, rawdata, date, networkavg, missingavg3mo, networkavg3mo, 
            imputed, imputed_param
                                                )
select raw.site, raw.date as rawdate, param as rawdata, imputed.date, networkavg, missingavg3mo, networkavg3mo, 
            imputed, case when param is null then imputed else param end as imputed_param 
from
(
SELECT biosmoke_pollution.stationdates_",town,"_",poll,".station as site, biosmoke_pollution.stationdates_",town,"_",poll,".date, ",poll,"_",stat," as param
                FROM
                biosmoke_pollution.stationdates_",town,"_",poll,"
                left join
                biosmoke_pollution.combined_pollutants
                on biosmoke_pollution.stationdates_",town,"_",poll,".station=biosmoke_pollution.combined_pollutants.site
                and biosmoke_pollution.stationdates_",town,"_",poll,".date=biosmoke_pollution.combined_pollutants.date
                                where biosmoke_pollution.stationdates_",town,"_",poll,".date >= ",mindate,"
                                        and biosmoke_pollution.stationdates_",town,"_",poll,".date <= '",maxdate,"'
                                        and biosmoke_pollution.stationdates_",town,"_",poll,".station = '",loc,"'
order by biosmoke_pollution.stationdates_",town,"_",poll,".date
) raw
left join
(
select t1.date,
        t1.networkavg,
        t2.missingavg3mo,
        t3,networkavg3mo,
        t1.networkavg*(t2.missingavg3mo/t3.networkavg3mo) as imputed
from ((biosmoke_pollution.networkavg t1
join
        biosmoke_pollution.missingavg3mo t2
        on t1.date=t2.date)
join
        biosmoke_pollution.networkavg3mo t3
        on t1.date=t3.date)
order by t1.date
) imputed
on raw.date=imputed.date
order by raw.date
",sep="")

#cat(txt)
strt=Sys.time()
dbSendQuery(ch,txt)              
endd=Sys.time()
print(endd-strt)


dbSendQuery(ch,"drop table biosmoke_pollution.networkavg ;")
dbSendQuery(ch,"drop table biosmoke_pollution.missingavg3mo;")
dbSendQuery(ch,"drop table biosmoke_pollution.networkavg3mo;")

}

}

#' @name n_missing
#' @title number missing
#' @param town the one to do
#' @param poll pollutant
#' @param thresh theshold below which we will do it
#' @return nmissing is a message like 'go for it'

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

#' @name citywide_av
#' @title city wide average
#' @param town
#' @param poll
#' @param stat
#' @return nothing to R, this creates things in the database
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

#' @name stitch_together
#' @title put all the bits together
#' @param poll pollutant
#' @return tables in the database
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

#' @name   missing99
#' @title   99th centile missing references of any type
#' @param poll pollutant
#' @return list of dates
missing99 <- function(poll){
dat <- dbSendQuery(ch,
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
grant all on pollution.",poll,"_to_check to biosmoke_group
",sep="")
)
return(dat)
}
