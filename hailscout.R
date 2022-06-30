###########################
#APPLICATION: HOMESITE HAIL WATCH
#DEVELOPER: DAN LITCHMORE
#LAST UPDATED: 8/1/2019
#PURPOSE: Consumes SPC hail reports from end of the current day. It then estimates how much PIF fall under these hail events.
# It then generates an email report sycned with a corresponding Tableau report.
##########################

#LIBRARIES##########################
library(sqldf)
library(odbc)
library(RSQLite)
library(ggplot2)
library(ggmap)
library(mgcv)
library(plyr)
library(magrittr)
library(kableExtra)
library(DT)
library(RODBC)
library(dplyr)
library(tidyr)
library(stringr)
library(yaml)
library(mailR)
library(sendmailR)
library(tidyverse)
library(ggalt)
library(ggthemes)
library(rgeos)
library(DBI)
library(RODBCext)
library(tidyr)
#####################################
gc()









#############DATE VARIABLE PREPARTATION########################

today <-Sys.Date() # remove the number to eventually just receive

dateplug <-format(today,format="%y%m%d")

emaildate <- format(today,format="%Y-%m-%d")

currentDate <- paste0("'",format(Sys.Date(),"%Y-%m-%d"),"'")

lastyear <-paste0("'",format(Sys.Date()-365,"%Y-%m-%d"),"'")
###############################################################


#################################HAIL DATA PREPARATION#########################################################################
haildata <- read.csv(paste0("https://www.spc.noaa.gov/climo/reports/",dateplug,"_rpts_hail.csv"),header=TRUE) 

if(nrow(haildata) > 0)  {
  tbdf <-data.frame(haildata$Time,haildata$Size/100,haildata$County,haildata$State,haildata$Lat,haildata$Lon,haildata$Comments)
  colnames(tbdf) <- c("Time CST","Size(in)","County","State","Lat","Lon","Comments") 
  eventlength <- length(tbdf$Comments)
  
  tbdfb <-tbdf[!(is.na(tbdf$Lon) | tbdf$Lon=="")  , ]
  
  tbdfa<- tbdfb[!(!as.numeric(tbdfb$Lat) | tbdfb$Lat=="")  , ]
  
  tbdfc <-tbdfa[!(!as.numeric(tbdfa$Lon) | tbdfa$Lon=="")  , ]
  #################################HAIL DATA PREPARATION#########################################################################
  
  
  
  #########SENDING INITIAL HAIL FALL DATA TO SQL################################################################################################################################
  write.table(tbdf,paste0('\\\\cambosnapp01\\CatMgt_DEV\\Perils\\Hail\\SPC_Daily_Collection\\SPC_Hail_Reporting\\SPC_Hail_Events\\temp_RealTime.csv'),row.names=FALSE,sep=",")
  
  dbhandle10 <- odbcDriverConnect('driver={SQL Server};server={camboscatmgt01\\CATMGT};database =Hail_Reporter;trusted_connection=true')
  
  clearjunk <- sqlQuery(dbhandle10,'drop table [Hail_Reporter].dbo.temp_RealTime')
  
  
  path <- paste0('\\\\cambosnapp01\\CatMgt_DEV\\Perils\\Hail\\SPC_Daily_Collection\\SPC_Hail_Reporting\\SPC_Hail_Events\\temp_RealTime.csv')
  
  X <-read.csv(path,header = TRUE, sep = ",")
  
  
  con <- dbConnect (odbc::odbc(),
                    Driver = "SQL Server",
                    Server = "{camboscatmgt01\\CATMGT}",
                    Database= "Hail_Reporter" #NAME OF THE DATABASE [Replace]
  )
  
  
  dbWriteTable (conn= con,
                overwrite =TRUE,
                name <- "temp_RealTime", #switch to actualfile if you want the exact file name inlcuding .csv extension
                value <- X) 
  #########SENDING INITIAL HAIL FALL DATA TO SQL################################################################################################################################ 
  
  
  
  ########PREPARING THE PIF DATA THAT WILLL BE COMINED WITH THE HAIL DATA#######################################################################################################
  
  dbhandle4 <- odbcDriverConnect('driver={SQL Server};server={CAMBOSCATMGT01\\CATMGT};database=Hail_Reporter;trusted_connection=true')
  
  ClearPIF <- sqlQuery(dbhandle4,' drop table [Hail_Reporter].dbo.Daily_PIF')
  
  dbhandle14 <- odbcDriverConnect('driver={SQL Server};server=cambosana03;database=ModelData;trusted_connection=true')
  
  #Policy information
  Pollive <- sqlQuery(dbhandle14,paste0('
                                        
                                        
                                        select 
                                        distinct
                                        [PolicyNumber]
                                        ,[PropertyStateCode]
                                        ,[PropertyRoofTypeCode] as RoofType
                                        ,[PropertyYearRoofInstalled] as RoofAge
                                        ,[PolicyEffectiveDate]
                                        ,[PropertyLatitude]
                                        ,[PropertyLongitude]
                                        ,[PropertyCounty]
                                        ,MAX([PolicyTerm]) as PolicyTerm
                                        from ModelData.dbo.[PolicyData]
                                        
                                        where PolicyEffectiveDate BETWEEN ',lastyear,' AND ',currentDate,'
                                        and (PolicyCancellationDate is null OR PolicyCancellationDate > ',currentDate,')
                                        and PolicyNumberTestFlag = 0
                                        and PolicyFormNumber = 3
                                        and PolicyExpirationDate > ',currentDate,' 
                                        and PolicyTerm > 0
                                        group by [PolicyNumber],[PropertyStateCode],[PropertyRoofTypeCode],[PropertyYearRoofInstalled],[PolicyEffectiveDate],[PropertyCounty],[PropertyLatitude],[PropertyLongitude]
                                        '))
  
  #Send it to SQL but define it's Server and database name 
  con2 <- dbConnect (odbc::odbc(),
                     Driver = "SQL Server",
                     Server = "{camboscatmgt01\\CATMGT}",
                     Database= "Hail_Reporter" #NAME OF THE DATABASE, change to your database
  )
  
  #Remove the pesky NAs!
  Pollive[is.na(Pollive)] <- 0
  
  #Give the PIF table the name 
  dbWriteTable (conn= con2,
                name <- "Daily_PIF",
                value <- Pollive)
  ########PREPARING THE PIF DATA THAT WILLL BE COMINED WITH THE HAIL DATA#######################################################################################################
  
  
  #####PERFORMING THE INTERSECTION BETWEEN THE HAIL FALL DATA AND THE PIF#######################################################################################################
  dbhandle1 <- odbcDriverConnect('driver={SQL Server};server={CAMBOSCATMGT01\\CATMGT};database=Hail_Reporter;trusted_connection=true')
  
  
  # Add buffers/geographies to both points 
  buffering <- sqlQuery(dbhandle1,paste0('
                                         
                                         
                                         
                                         drop table [Hail_Reporter].[dbo].temp_Hail_buffer
                                         drop table [Hail_Reporter].[dbo].[temp_Hail_PIF]
                                         
                                         select *, Geompoint.STBuffer(6000) as buff_10km
                                         into [Hail_Reporter].[dbo].[temp_Hail_buffer]  --pull from here when preforming the join (need to run again to actually make the table)
                                         from
                                         ( SELECT [Time.CST],[State], [Comments],[Size.in.],geography::Point(Lat,Lon,4269) as Geompoint
                                         FROM [Hail_Reporter].[dbo].[temp_RealTime]
                                         
                                         ) U
                                         
                                         
                                         SELECT 
                                         a.[PolicyNumber]
                                         ,a.[PropertyStateCode] as [State]
                                         , a.[RoofType] as RoofType
                                         , a.[RoofAge] as RoofAge
                                         , a.[PolicyTerm] as PolicyTerm
                                         ,geography::Point(a.[PropertyLatitude],a.[PropertyLongitude],4269) as propgeom
                                         into [Hail_Reporter].dbo.[temp_Hail_PIF] -- pull from here when preforming the join
                                         FROM [Hail_Reporter].[dbo].[Daily_PIF] a'))
  
  
  
  
  #####PERFORMING THE INTERSECTION BETWEEN THE HAIL FALL DATA AND THE PIF####################################################################################################### 
  
  #TRANSFER 7!!!  SO HERE IS WHERE THE BREAD AND BUTTER QUERY IS RAN, date appendages are restored here
  dbhandle2 <- odbcDriverConnect('driver={SQL Server};server={CAMBOSCATMGT01\\CATMGT};database=Hail_Reporter;trusted_connection=true')
  
  
  #Do Max(PolicyTerm) here 
  Joindata <- sqlQuery(dbhandle2,paste0('  
                                        select
                                        distinct
                                        Count(distinct h.[Size.in.]) as [Impact_Events]
                                        ,ROUND (Avg(cast(h.[Size.in.] as Float)),2) as AvgHailSize
                                        ,max(h.[Size.in.]) as MxHailSize
                                        ,g.[PolicyNumber] as [PolicyNumber]
                                        ,g.[RoofType] as [RoofType]
                                        ,g.[RoofAge] as [RoofAge]
                                        ,g.[PolicyTerm] as [PolicyTerm]
                                        into [Hail_Reporter].dbo.[Hail_',dateplug,']
                                        from [Hail_Reporter].dbo.[temp_Hail_PIF] g
                                        inner join [Hail_Reporter].dbo.temp_Hail_buffer h  on  g.[State] = h.[State] 
                                        where g.propgeom.STIntersects(h.buff_10km)=1
                                        group by g.[PolicyNumber], g.[RoofType], g.[RoofAge],g.PolicyTerm
                                        '))
  
  #####SUCESS, okay go home now
  JoindataWState <- sqlQuery(dbhandle2,paste0(' 
                                              
                                              select
                                              distinct
                                              Count(distinct h.[Size.in.]) as [Impact_Events]
                                              ,ROUND (Avg(cast(h.[Size.in.] as Float)),2) as AvgHailSize
                                              ,max(h.[Size.in.]) as MxHailSize
                                              ,g.[PolicyNumber] as [PolicyNumber]
                                              ,g.[RoofType] as [RoofType]
                                              ,g.[RoofAge] as [RoofAge]
                                              ,g.[PolicyTerm] as [PolicyTerm]
                                              ,g.[State] as [StateCode]
                                              from [Hail_Reporter].dbo.[temp_Hail_PIF] g
                                              inner join [Hail_Reporter].dbo.temp_Hail_buffer h  on  g.[State] = h.[State] 
                                              where g.propgeom.STIntersects(h.buff_10km)=1
                                              group by g.[PolicyNumber], g.[RoofType], g.[RoofAge],g.PolicyTerm,g.[State]
                                              '))
  
  
  
  
  # TRANSFER...8? I don't really remember, all we did here was extract the affected policies vs hail data from SQL and now it's in R
  
  #DRAW STUFF FROM SQL #JOINEDPOLICYDATA will be the sample we work with
  dbhandle3 <- odbcDriverConnect('driver={SQL Server};server=cambosana03;database=ModelData;trusted_connection=true;',rows_at_time = 1) #simply setting up the database connection in SQL
  dbhandle40 <- odbcDriverConnect('driver={SQL Server};server={CAMBOSCATMGT01\\CATMGT};database=Hail_Reporter;trusted_connection=true;',rows_at_time = 1)
  res2 <- sqlQuery(dbhandle40,paste0('select * from [Hail_',dateplug,']')) #Name of the data table you want to extract
  
  
  
  ###Make a new res2 that adds affected policy information
  
  
  
  
  
  
  
  
  JoinedPolicyData<-data.frame(res2) # THIS IS THE KEY CSV, THIS IS WILL CONTAIN THE HAIL VS POLICY INFORMATION
  
  
  #THIS IS WHERE THE AMMENDMENTS ARE STORED
  PriorClaims <- sqlQuery(dbhandle3,'SELECT distinct
                          count (distinct a.[hsClaimId]) as priorClaims
                          --,count(distinct a.[hsClaimNumber]) ClaimsPrior
                          ,a.[PolicyNumber]
                          FROM [ModelData].[dbo].[ClaimData] a left join
                          [ModelData].dbo.[PolicyData] b on a.[PolicyNumber] = b.[PolicyNumber]
                          group by a.[PolicyNumber]
                          order by priorClaims desc')
  
  PriorClaims <- unique(PriorClaims)
  
  
  ##################################
  #                                #
  ##################################
  
  
  ClaimsnHailData <- merge(x=JoindataWState,y=PriorClaims,by = "PolicyNumber",all.x=TRUE)
  
  ClaimsnHailData <- data.frame(ClaimsnHailData)
  
  ClaimsnHailData[is.na(ClaimsnHailData)] <- 0
  
  #TRANSFER 9 THIS IS WHERE WE WORK TO GET THAT VERY TABLE BACK INTO A CSV THAT WILL GO TOWARDS THE EMAIL
  
  #There seems to be one redundant path so we may want to keep only the bottom path and send that to a new email folder we have set up  (Hail_Affected_PIF?)
  
  #This all works
  
  write.table(JoinedPolicyData,paste0('\\\\cambosnapp01\\CatMgt_DEV\\Perils\\Hail\\SPC_Daily_Collection\\SPC_Hail_Reporting\\Hail_Affected_PIF\\Hail_Affected_PIF',dateplug,'.csv'),row.names=FALSE,sep=",")#Establish the file end 
  
  
  write.table(ClaimsnHailData,paste0('\\\\cambosnapp01\\CatMgt_DEV\\Perils\\Hail\\SPC_Daily_Collection\\SPC_Hail_Reporting\\Hail_Affected_PIF\\Hail_Affected_PIF_PriorClaims',dateplug,'.csv'),row.names=FALSE,sep=",")#Establish the file end 
  #SUCCESS
  
  #TRANSFER 10 replace with new file path, should be easy
  
  #All of this just to get a number for the affected policies lmao
  
  howmany<- data.frame(read.csv(paste0('\\\\cambosnapp01\\CatMgt_DEV\\Perils\\Hail\\SPC_Daily_Collection\\SPC_Hail_Reporting\\Hail_Affected_PIF\\Hail_Affected_PIF',dateplug,'.csv')))
  
  thismany <- length(howmany$PolicyNumber)
  
  #SUCCESS
  
  
  #EMAIL COMES HERE
  msg <- mime_part(paste0('<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0
                          Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                          <html xmlns="http://www.w3.org/1999/xhtml">
                          <head>
                          <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                          <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
                          <title>Distance To Fire Report</title>
                          </head>
                          <style>body{font-family:Helvetica,\"sans-serif\";}
                          table{border-left:1px solid #000000;border-top:1px solid #000000;}
                          table td{border-right:1px solid #000000;border-bottom:1px solid #000000;font-size:16px; font-weight:normal;}
                          table th{border-right:1px solid #000000;border-bottom:1px solid #000000;font-size:16px; font-weight:bold;}
                          </style>
                          <body>
                          <p>Hello,<br><br>Above delineates Homesite exposure potentially affected by hail as of ',emaildate, '.
                          Attached are ',thismany,' of these in-force policies in a csv format.<br> To view an interactive report of the events on that day click <a href="https://tableau.homesite.com/#/views/HHWDailyReport/HHWDailyUpdate?:iid=1">here</a> .<br> 
                          Please reach out to Cat Management if you need additional information.<br><br>Thanks!<br><br></p>
                          </body>
                          </html>'))
  
  # Override content type
  msg[["headers"]][["Content-Type"]] <- "text/html"
  
  
  
  
  
  #body <- paste0("The Affected properties and map of hail incidents are posted above, enjoy your ",format(Sys.time(),"%A")) old body
  
  
  #TRANSFER 11, have the email attachment script pull directly from SPC_HAIL's two main folder sinks
  
  
  #DATA TABLE TO EMAIL    
  attachmentPath <-paste0('\\\\cambosnapp01\\CatMgt_DEV\\Perils\\Hail\\SPC_Daily_Collection\\SPC_Hail_Reporting\\Hail_Affected_PIF\\Hail_Affected_PIF_PriorClaims',dateplug,'.csv')
  pathsplit <- unlist(strsplit(attachmentPath, split = "\\\\"))
  attachmentName <- pathsplit[length(pathsplit)]
  attachmentObject1 <- mime_part(x=attachmentPath,name = attachmentName)
  
  
  #POINT MAP TO EMAIL
  #attachmentPath2 <- paste0('\\\\cambosnapp01\\CatMgt_DEV\\Perils\\Hail\\SPC_Daily_Collection\\SPC_Hail_Reporting\\Hail_Affected_Area\\Hail_Affected_Area',dateplug,'.jpg')
  #pathsplit2 <- unlist(strsplit(attachmentPath2, split = "\\\\"))
  #attachmentName2 <- pathsplit2[length(pathsplit2)]
  #attachmentObject2 <- mime_part(x=attachmentPath2,name = attachmentName2)
  
  
  
  bodyWithAttachment <- list(msg,attachmentObject1)    #,attachmentObject2)
  
  apo <- "'"
  
  sender <- "HOMESITE_HAIL_WATCH@Homesite.com"
  #recipients <- c("daniel.litchmore@homesite.com")
  
  recipients <- c("daniel.litchmore@homesite.com","jiaxin.yu@homesite.com","benjamin.spaulding@homesite.com","NFries@homesite.com","darrel.barbato@homesite.com","anir.bhattacharyya@homesite.com","keith.simard@homesite.com","gary.harvell@homesite.com","travis.holt@homesite.com","raymond.cook@homesite.com","judith.taylor@homesite.com","dean.mirabito@homesite.com","quinn.saner@homesite.com","anna.kervison@homesite.com","julie.lyons@homesite.com","sstayton@homesite.com","samantha.hoch@homesite.com")
  #recipients <- c("daniel.litchmore@homesite.com","jiaxin.yu@homesite.com","benjamin.spaulding@homesite.com")
  #today <- Sys.Date()
  title.string <- paste0('Homesite Exposure Potentially Affected by Hail ',format(today,format = "%Y-%m-%d"))
  server <- "hsboscas.camelot.local"
  
  mailControl <- list (smtpServer =server)
  sendmail(from = sender,
           to = recipients,
           inline= TRUE,
           subject = title.string,
           msg = bodyWithAttachment, #send image
           control = mailControl)
  
  
  print(paste0("Email Sent playah",dateplug))
  
  
  #NULL STATEMENT INCOMING  
  
                          } else {
                            # Just ends the entire process
                            
                            
                            
                            
                            #EMAIL COMES HERE
                            msg <- mime_part(paste0('<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0
                                                    Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                                                    <html xmlns="http://www.w3.org/1999/xhtml">
                                                    <head>
                                                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                                                    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
                                                    <title>Distance To Fire Report</title>
                                                    </head>
                                                    <style>body{font-family:Helvetica,\"sans-serif\";}
                                                    table{border-left:1px solid #000000;border-top:1px solid #000000;}
                                                    table td{border-right:1px solid #000000;border-bottom:1px solid #000000;font-size:16px; font-weight:normal;}
                                                    table th{border-right:1px solid #000000;border-bottom:1px solid #000000;font-size:16px; font-weight:bold;}
                                                    </style>
                                                    <body>
                                                    <p>Hello,<br><br> No hail events were detected today.<br>Thank you.</p>
                                                    </body>
                                                    </html>'))
                            
                            # Override content type
                            msg[["headers"]][["Content-Type"]] <- "text/html"
                            
                            
                            
                            
                            
                            #body <- paste0("The Affected properties and map of hail incidents are posted above, enjoy your ",format(Sys.time(),"%A")) old body
                            
                            
                            #TRANSFER 11, have the email attachment script pull directly from SPC_HAIL's two main folder sinks
                            
                            
                            
                            bodyWithAttachment <- list(msg)    #,attachmentObject2)
                            
                            apo <- "'"
                            
                            sender <- "HOMESITE_HAIL_WATCH@Homesite.com"
                            # recipients <- c("daniel.litchmore@homesite.com")
                            
                            recipients <- c("daniel.litchmore@homesite.com","jiaxin.yu@homesite.com","benjamin.spaulding@homesite.com","NFries@homesite.com","darrel.barbato@homesite.com","anir.bhattacharyya@homesite.com","keith.simard@homesite.com","gary.harvell@homesite.com","travis.holt@homesite.com","raymond.cook@homesite.com","judith.taylor@homesite.com","dean.mirabito@homesite.com","quinn.saner@homesite.com","julie.lyons@homesite.com","sstayton@homesite.com","samantha.hoch@homesite.com","xiaoying.zhu@homesite.com","katelyn.reilly@homesite.com","elizabeth.hawley@homesite.com","elizabeth.hawley@homesite.com")
                            #recipients <- c("daniel.litchmore@homesite.com","jiaxin.yu@homesite.com","benjamin.spaulding@homesite.com")
                            #today <- Sys.Date()
                            title.string <- paste0('Homesite Exposure Potentially Affected by Hail ',format(today,format = "%Y-%m-%d"))
                            server <- "hsboscas.camelot.local"
                            
                            mailControl <- list (smtpServer =server)
                            sendmail(from = sender,
                                     to = recipients,
                                     inline= TRUE,
                                     subject = title.string,
                                     msg = bodyWithAttachment, #send image
                                     control = mailControl)
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            print(paste0("No hail Events for Yesterday ",today))
                            
                            
                                                    } 

gc()

