
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
