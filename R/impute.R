
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
