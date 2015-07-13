
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
