
todo=cbind(towns,rep('pm10',length(towns)),c("'1997-05-23'","'1994-01-01'","'1994-02-15'",
"'1994-02-02'","'2006-04-22'" ,"'2001-05-01'"))

todo=rbind(todo,cbind(towns,rep('pm25',length(towns)),c("'1994-02-15'","'1996-05-07'","'1998-03-01'" ,"'1996-06-19'","'2006-06-05'" ,"'2005-06-04'")))

todo=rbind(todo,cbind(towns[1:4],rep('o3',4),rep("'1994-01-01'",4)))

todo=as.data.frame(todo)
todo
todo$stat=ifelse(todo[,2]=='o3','max','av')
todo

i=1
town=todo[i,1]
poll=todo[i,2]
mindate=todo[i,3]
stat=todo[i,4]

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

sitelist=sites_todo(town=town,mindate=mindate,poll=poll,stat=stat)

#sitelist

impute(sitelist, town, poll, stat)


nmissed=n_missing(town,poll)
print(nmissed)
if(nmissed=='go for it'){
        citywide_av(town,poll,stat)
        }
        
}


# clean up

dbSendQuery(ch,
# cat(
paste("drop table pollution.",'pm10',"_",c('av'),"_events_",gsub('Lower Hunter','Newcastle',towns),"_temp",sep='',collapse=';\n'))

dbSendQuery(ch,
# cat(
paste("drop table pollution.",'pm25',"_",c('av'),"_events_",gsub('Lower Hunter','Newcastle',towns),"_temp",sep='',collapse=';\n'))

dbSendQuery(ch,
# cat(
paste("drop table pollution.",'o3',"_",c('max'),"_events_",gsub('Lower Hunter','Newcastle',towns[1:4]),"_temp",sep='',collapse=';\n'))

stitch_together(poll=polls[5,3])
stitch_together(poll=polls[7,3])
stitch_together(poll=polls[4,3])
