
library(odbc)

con <- dbConnect(odbc::odbc(),
                 Driver = "",
                 Server = "",
                 Database = "",
                 UID = "",
                 PWD = "")


#query <- "SELECT DATEADD(day, -1, CAST(GETDATE() AS DATE)) AS fecha_actualizacion, CONVERT(VARCHAR, GETDATE(), 108) AS hora_actualizacion, CAST(FECHA_MONIT AS DATE) AS fecha_orbit, SUM(total) AS inserciones FROM EXPORTA_mm WHERE CAST(FECHA_MONIT AS DATE) BETWEEN DATEADD(day, -11, CAST(GETDATE() AS DATE)) AND DATEADD(day, -1, CAST(GETDATE() AS DATE)) GROUP BY CAST(FECHA_MONIT AS DATE) ORDER BY CAST(FECHA_MONIT AS DATE)"
#query <- "SELECT CAST(GETDATE() AS DATE) AS fecha_actualizacion, CONVERT(VARCHAR, GETDATE(), 108) AS hora_actualizacion, CAST(FECHA_MONIT AS DATE) AS fecha_orbit, SUM(total) AS inserciones FROM EXPORTA_mm WHERE CAST(FECHA_MONIT AS DATE) BETWEEN DATEADD(day, -10, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE) GROUP BY CAST(FECHA_MONIT AS DATE) ORDER BY CAST(FECHA_MONIT AS DATE)"
query
sql <- dbGetQuery(con, query)

sql <- data.frame(sql)
sql$fecha_orbit = as.Date(sql$fecha_orbit)
sql$fecha_actualizacion <- as.Date(sql$fecha_actualizacion)

View(sql)

path <- "C:/Users/miguel.sanchez/Desktop/Downloader/"
inserts <- paste(path, "conteo_orbit.csv", sep = "")


sapply(sql, class)

View(sql)

if(file.exists(inserts)){
  tab <- as.data.frame(read.csv(file = inserts,  sep = "|"))
  tab$fecha_orbit = as.Date(tab$fecha_orbit)
  tab$fecha_actualizacion <- as.Date(tab$fecha_actualizacion)
  
  View(tab)
  
  tab <- rbind(tab, sql)
  tab <- tab[
    with(tab, order(tab$fecha_actualizacion, decreasing = FALSE)),
  ]
  
  tab <- tab[
    with(tab, order(tab$fecha_orbit, decreasing = FALSE)),
  ]
  
  tab <- tab[!duplicated(tab[c("fecha_actualizacion", "fecha_orbit")]), ]
  write.table(tab, file = inserts, sep = "|", row.names = FALSE)
}else{
  write.table(sql, file = inserts, sep = "|", row.names = FALSE)
}


View(tab)

dbDisconnect(con)
