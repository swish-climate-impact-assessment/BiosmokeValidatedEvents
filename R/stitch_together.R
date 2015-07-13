#' @name stitch_together
#' @title put all the bits together
#' @param poll pollutant
#' @param stat av or max
#' @return tables in the database
stitch_together <- function(poll=polls[5,3], stat = 'av'){

print(poll)

# NB only once!
try(
exist<- dbGetQuery(ch,
#cat(
paste("select * from biosmoke_pollution.",poll,"_",stat,"_events_all_regions limit 1",sep='')
), silent=T)

if(length(nrow(exist))==0){

        dbSendQuery(ch,
        #cat(
        paste("CREATE TABLE biosmoke_pollution.",poll,"_",stat,"_events_all_regions
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
try(
exist<- dbGetQuery(ch,
#cat(
paste("select * from biosmoke_pollution.",poll,"_",stat,"_events_",town," limit 1",sep='')
), silent=T)

if(length(nrow(exist))>0){
        
        # dbSendQuery(ch,
        # # cat(
        # paste("delete from biosmoke_pollution.",poll,"_",stat,"_events_all_regions where region = \'",town,"\'",sep="")
        # )

        dbSendQuery(ch,
        # cat(
        paste("insert into biosmoke_pollution.",poll,"_",stat,"_events_all_regions (region, date, ",poll,"_",stat,", ranked, pctile)
        select '",town,"', date, ",poll,"_",stat,", ranked, pctile
        from  biosmoke_pollution.",poll,"_",stat,"_events_",town,sep="")
        )

}
rm(exist)

}

}
