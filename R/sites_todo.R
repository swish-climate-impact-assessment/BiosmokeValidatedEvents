#' @name sites_todo
#' @title sites with potential
#' @param town
#' @param mindate
#' @param maxdate
#' @param threshold
#' @param poll
#' @param stat
#' @return text for a sql query

sites_todo <- function(town, mindate, maxdate="2007-12-31", threshold=0.7, poll, stat){

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
                                        and biosmoke_pollution.stationdates_",town,"_",poll,".date <= '",maxdate,"'
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
        where polls.date >= ",mindate," and polls.date <= '",maxdate,"'
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
