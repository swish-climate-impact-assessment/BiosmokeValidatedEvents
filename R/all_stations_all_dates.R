
#' @name all_stations_all_dates
#' @title All Stations, All Dates
#' @param town Biomass Study area
#' @param pollutant you got it
#' @return text for a query
all_stations_all_dates <- function(town, pollutant){
if(length(grep("_av$", pollutant)) > 0){
  pollutant_label <- gsub("_av$", "", pollutant)
} else {
  pollutant_label <- pollutant
}

txt <- paste("
select site as station, date 
into biosmoke_pollution.stationdates_",town,"_",pollutant_label,"
from
(select distinct biosmoke_pollution.combined_pollutants.site 
from biosmoke_pollution.combined_pollutants
join
        (
        select t1.site,t2.studysite as region
        from biosmoke_pollution.pollution_stations_combined_final t1 , 
        biosmoke_spatial.study_slas_01 t2
        where st_intersects(t1.geom,t2.geom) and upper(t2.studysite) like '",toupper(town),"%'
        order by studysite
        ) combined_pollutants2
on biosmoke_pollution.combined_pollutants.site=combined_pollutants2.site
) sites,
(select * from alldates_",pollutant_label,"_",town,") dates
",sep="")

# cat(txt)
return(txt)
}
