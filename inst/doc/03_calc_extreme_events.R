
#### name:03_calc_extreme_events.R ####
# now make a view for each poll so we can see what has been checked and what still needs to be checked

for(poll in c("pm10_av", "pm25_av", "o3_max")){
#poll = "pm10_av"
txt <-  paste("
create or replace view biosmoke_events.",poll,"_checked
as 
select ",poll,".region, ",poll,".date, cast(",poll,".pctile*100 as integer) as pctile, refid, eventid
from
biosmoke_pollution.",poll,"_events_all_regions as ",poll,"
left join
(
        SELECT t1.date, t2.*
        FROM 
                biosmoke_pollution.",poll,"_events_all_regions t1
        ,
                (
                select tab1.*, 
                case when place like 'Sydney%' then 'Sydney' else place end as region,
                field3,field5, field7 from
                biosmoke_events.tblevents tab1
                join biosmoke_events.tblreferences tab2
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
where pctile>=.95 and mindate is not null 
  ORDER BY ",poll,".region, ",poll,".pctile DESC;
grant select on biosmoke_events.",poll,"_checked to biosmoke_user;

create or replace view biosmoke_events.",poll,"_to_check
as 
select ",poll,".region, ",poll,".date, cast(",poll,".pctile*100 as integer) as pctile, refid, eventid
from
biosmoke_pollution.",poll,"_events_all_regions as ",poll,"
left join
(
        SELECT t1.date, t2.*
        FROM 
                biosmoke_pollution.",poll,"_events_all_regions t1
        ,
                (
                select tab1.*, 
                case when place like 'Sydney%' then 'Sydney' else place end as region,
                field3,field5, field7 from
                biosmoke_events.tblevents tab1
                join biosmoke_events.tblreferences tab2
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
where pctile>=.95 and mindate is null 
  ORDER BY ",poll,".region, ",poll,".pctile DESC;
grant select on biosmoke_events.",poll,"_to_check to biosmoke_user
",sep="")

cat(txt)
dbSendQuery(ch, txt)
}
