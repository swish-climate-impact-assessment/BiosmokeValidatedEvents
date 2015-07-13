#' @name   missing99
#' @title   99th centile missing references of any type
#' @param poll pollutant
#' @return list of dates
missing99 <- function(poll){
dat <- dbSendQuery(ch,
# cat(
paste("
create or replace view biosmoke_pollution.",poll,"_to_check
as 
select ",poll,".*, eventid,refid, eventtype, place,mindate,maxdate, field3,field5, field7
from
biosmoke_pollution.",poll,"_av_events_all_regions as ",poll,"
left join
(
        SELECT t1.date, t2.*
        FROM 
                biosmoke_pollution.",poll,"_",stat,"_events_all_regions t1
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
grant all on biosmoke_pollution.",poll,"_to_check to biosmoke_group
",sep="")
)
return(dat)
}
