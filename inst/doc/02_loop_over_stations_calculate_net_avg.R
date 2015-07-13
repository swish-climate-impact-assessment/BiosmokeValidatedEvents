#### sites_todo
txt <- sites_todo(town=town,mindate=mindate,poll=poll,stat=stat)
cat(txt)
sitelist <- dbGetQuery(ch, txt)[,1]
sitelist
impute(sitelist, town, poll, stat)
# no avg all sites per day for city wide averages  
# AND fill any missing days with avg of before and after (if this is less than 5% of days)
# first make sure the number of missing days with one valid either side is < 5% of total days
n_missing(town,poll)

# if = 'go for it'
citywide_av(town,poll,stat)
for(i in 2:nrow(todo)){
# i=15
town=todo[i,1]
if(town=="Lower Hunter"){
        town='Newcastle'
        } else {
        town=todo[i,1]
        }
print(town)     
poll=todo[i,2]
print(poll)
mindate=todo[i,3]
print(mindate)
stat=todo[i,4]
print(stat)

txt <- sites_todo(town=town,mindate=mindate,poll=poll,stat=stat)
sitelist <- dbGetQuery(ch, txt)[,1]
#sitelist

impute(sitelist, town, poll, stat)


nmissed=n_missing(town,poll)
print(nmissed)
if(nmissed=='go for it'){
        citywide_av(town,poll,stat)
        }
        
}
stitch_together(poll="PM10", stat = "av")
stitch_together(poll="PM25", stat = "av")
stitch_together(poll="O3", stat = "max")
dbSendQuery(ch,'grant all on table biosmoke_pollution.pm10_av_events_all_regions to biosmoke_user')   
dbSendQuery(ch,'grant all on table biosmoke_pollution.pm25_av_events_all_regions to biosmoke_user')
dbSendQuery(ch,'grant all on table biosmoke_pollution.o3_max_events_all_regions to biosmoke_user')
