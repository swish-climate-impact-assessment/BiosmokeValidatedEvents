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
 t1.date, avg(t2.",poll,") as citywide_",poll," , count(*)
from
        (
        select date , avg(imputed_param) as ",poll,"
        from biosmoke_pollution.imputed_",poll,"_",town,"
        group by date
        having avg(imputed_param) is null
        ) t1,
        (
        select date , avg(imputed_param) as ",poll,"
        from biosmoke_pollution.imputed_",poll,"_",town,"
        group by date
        ) t2
where (t2.date >= t1.date-1 and  t2.date <= t1.date+1)
group by t1.date
having count(t2.",poll,")>1
order by t1.date
) foo
",sep="")
)

noverall<- dbGetQuery(ch,
#cat(
paste("select count(*) from
(
select date , avg(imputed_param) as ",poll,"
from biosmoke_pollution.imputed_",poll,"_",town,"
group by date
) bar",sep="")
)

if(nmissing/noverall<=thresh){"go for it"} else {"don't do the avg of the missing dates with before and after, too many"}

}
