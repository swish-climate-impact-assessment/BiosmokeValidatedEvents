# clean up

dbSendQuery(ch,
# cat(
paste("drop table biosmoke_pollution.",'pm10',"_",c('av'),"_events_",gsub('Lower Hunter','Newcastle',towns),"_temp",sep='',collapse=';\n'))

dbSendQuery(ch,
# cat(
paste("drop table biosmoke_pollution.",'pm25',"_",c('av'),"_events_",gsub('Lower Hunter','Newcastle',towns),"_temp",sep='',collapse=';\n'))

dbSendQuery(ch,
# cat(
paste("drop table biosmoke_pollution.",'o3',"_",c('max'),"_events_",gsub('Lower Hunter','Newcastle',towns[1:4]),"_temp",sep='',collapse=';\n'))

tbls <- pgListTables(ch, "biosmoke_pollution")
tbls
# to keep
"
4                combined_pollutants biosmoke_pollution
52         o3_max_events_all_regions biosmoke_pollution
53        pm10_av_events_all_regions biosmoke_pollution
54        pm25_av_events_all_regions biosmoke_pollution
3  pollution_stations_combined_final biosmoke_pollution
"
tbls <- read.table(textConnection("rowid                        relname            nspname
48              imputed_o3_illawarra biosmoke_pollution
50              imputed_o3_newcastle biosmoke_pollution
44                  imputed_o3_perth biosmoke_pollution
46                 imputed_o3_sydney biosmoke_pollution
16               imputed_pm10_hobart biosmoke_pollution
11            imputed_pm10_illawarra biosmoke_pollution
18           imputed_pm10_launceston biosmoke_pollution
14            imputed_pm10_newcastle biosmoke_pollution
7                 imputed_pm10_perth biosmoke_pollution
9                imputed_pm10_sydney biosmoke_pollution
24               imputed_pm25_hobart biosmoke_pollution
2             imputed_pm25_illawarra biosmoke_pollution
26           imputed_pm25_launceston biosmoke_pollution
6             imputed_pm25_newcastle biosmoke_pollution
20                imputed_pm25_perth biosmoke_pollution
22               imputed_pm25_sydney biosmoke_pollution
49           o3_max_events_illawarra biosmoke_pollution
51           o3_max_events_newcastle biosmoke_pollution
45               o3_max_events_perth biosmoke_pollution
47              o3_max_events_sydney biosmoke_pollution
17             pm10_av_events_hobart biosmoke_pollution
12          pm10_av_events_illawarra biosmoke_pollution
19         pm10_av_events_launceston biosmoke_pollution
15          pm10_av_events_newcastle biosmoke_pollution
8               pm10_av_events_perth biosmoke_pollution
10             pm10_av_events_sydney biosmoke_pollution
25             pm25_av_events_hobart biosmoke_pollution
5           pm25_av_events_illawarra biosmoke_pollution
43         pm25_av_events_launceston biosmoke_pollution
23          pm25_av_events_newcastle biosmoke_pollution
21              pm25_av_events_perth biosmoke_pollution
1              pm25_av_events_sydney biosmoke_pollution
39          stationdates_hobart_pm10 biosmoke_pollution
40          stationdates_hobart_pm25 biosmoke_pollution
35         stationdates_illawarra_o3 biosmoke_pollution
33       stationdates_illawarra_pm10 biosmoke_pollution
34       stationdates_illawarra_pm25 biosmoke_pollution
41      stationdates_launceston_pm10 biosmoke_pollution
42      stationdates_launceston_pm25 biosmoke_pollution
38         stationdates_newcastle_o3 biosmoke_pollution
36       stationdates_newcastle_pm10 biosmoke_pollution
37       stationdates_newcastle_pm25 biosmoke_pollution
29             stationdates_perth_o3 biosmoke_pollution
27           stationdates_perth_pm10 biosmoke_pollution
28           stationdates_perth_pm25 biosmoke_pollution
32            stationdates_sydney_o3 biosmoke_pollution
30          stationdates_sydney_pm10 biosmoke_pollution
31          stationdates_sydney_pm25 biosmoke_pollution
"), header = T)

head(tbls)

for(i in 1:nrow(tbls)){
#i = 1
  dbSendQuery(ch,
#cat(
paste("drop table biosmoke_pollution.",tbls$relnam[i],sep='')
  )

}
