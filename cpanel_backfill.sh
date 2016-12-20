#!/bin/sh



kinit -R -k -t /home/hdp_oc_team/kerberos/hdp_oc_team.keytab hdp_oc_team@DC1.CORP.GD

##hive -f /home/hdp_oc_team/cpanel_backfill/cpanel_backfill.HQL;

##hadoop jar ${HOME}/tdch-1.1.1.jar com.teradata.hadoop.tool.TeradataExportTool -classname com.teradata.jdbc.TeraDriver -url jdbc:teradata://godaddy3.prod.phx3.gdg/DATABASE=P_HadoopImp_S -username P_BTCH_HadoopImp -password P_BTCH_HadoopImp -jobtype hdfs -fileformat textfile -method multiple.fastload -batchsize 20000 -separator "|" -sourcepaths /user/hdp_oc_team/hdp_oc_team.db/prod_events -targettable  STG_PROD_EVENTS -nullnonstring "\N" -targetfieldnames "SHOPPER_ID,RESOURCE_ID,PROD_TYPE,EVENT_DATE_TXT,EVENT_ACTION";
bteq -c ASCII < /home/hdp_oc_team/cpanel_backfill/cpanel_backfill.btq  
  
