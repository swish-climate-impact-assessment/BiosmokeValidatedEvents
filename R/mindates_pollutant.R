#' @name mindates_pollutant
#' @title Minimum Date a Pollutant is observed from 
#' @param town the Biomass Study Town in question
#' @param pollutant you got it
#' @return text for a SQL query


mindates_pollutant <- function(
  town = "perth"
  ,
  pollutant = "pm10_av"
  ){
  if(length(grep("_av$", pollutant)) > 0){
    pollutant_label <- gsub("_av$", "_avg", pollutant)
  } else {
    pollutant_label <- pollutant
  }
txt <- paste("select t1.r2, min(t1.date) as min",pollutant_label,"
      from (
        SELECT combined_pollutants2.r2, date, avg(",pollutant,") as ", pollutant_label, "
        FROM biosmoke_pollution.combined_pollutants 
        join 
        (
                select t1.site,t1.region as r2, t2.studysite as region
                from biosmoke_pollution.pollution_stations_combined_final t1,
                biosmoke_spatial.study_slas_01 t2
                where st_intersects(t1.geom,t2.geom)
                  and lower(
      case when t2.studysite like \'Sydney%\' then \'Sydney\' else t2.studysite end 
                   ) = \'",tolower(town),"\'
                order by studysite
        ) combined_pollutants2 
        on biosmoke_pollution.combined_pollutants.site = combined_pollutants2.site
        where ",pollutant," is not null
        group by r2,date
        order by r2, date) t1
      group by t1.r2
  ", sep = "")
#cat(txt)
  return(txt)
}
