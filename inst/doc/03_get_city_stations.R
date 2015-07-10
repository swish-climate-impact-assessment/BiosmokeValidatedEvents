
# now get the pollution stations
#town=towns[2]
print(town)
layer  <- paste("sites_", tolower(town), sep  = "")
txt <- paste("select t1.*, t2.studysite as region2
  into ",user,".",layer,"
  from spatial.pollution_stations_combined_final t1,
  health.study_slas_01 t2
  where st_intersects(t1.gda94_geom,t2.the_geom) and
  upper(region) like '",toupper(town),"%'"
             , sep = "")
cat(txt)




txt2 <- paste("\"C:\\Program Files\\PostgreSQL\\8.3\\bin\\pgsql2shp\" -f \"",town,"\" -h ",postgis_ipaddress,"  -u ",user,"  -P ",passphrase," biomass \"select * from ",layer,"\"", sep="")
sink("dopgshp.bat")
cat(txt2)
sink()
#
system("dopgshp.bat")
file.remove("dopgshp.bat")
sites <- readShapePoints(paste(town,".shp",sep=""))
#cleanup_shp(town=town)

town

#sites_perth=sites
sites_sydney=sites
#sites_illawarra=sites
#sites_hunter=sites

sink("dopgshp.bat")
cat(paste("\"C:\\Program Files\\PostgreSQL\\8.3\\bin\\pgsql2shp\" -f \"",town,"\" -h ",postgis_ipaddress," -u ivan_hanigan -P ",passphrase," weather \"select * from public.oz_coast\"",sep=""))
sink()
#
system("dopgshp.bat")
file.remove("dopgshp.bat")
coast = readShapeSpatial(paste(town,".shp",sep=""))
cleanup_shp(town=town)
