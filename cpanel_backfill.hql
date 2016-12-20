SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;



With dcr_cpnl as
(
select dcr.shopper_id, er.resource_id, case when dcr.e_id = 'hosting.cpanel.api.customer_migration.complete' then 'Migrate' when dcr.e_id = 'hosting.cpanel.account.setup.complete' then 'Setup' else 'Publish' end as event_action, 'cPanel' as prod_type, max(e_time) as event_date
  from dm_ecommerce.fact_billing_external_resource er
  join 
  (select * from dm_customer_interaction.dm_user_events dcr
   where e_id in('hosting.cpanel.api.customer_migration.complete', 'hosting.cpanel.account.setup.complete', 'hosting.cpanel.account.publish')
   
   AND dcr.year   = 2016
   AND dcr.month  <= 7
   
   --AND dcr.year   = 2015
   --AND dcr.month  = 7
        
   ) dcr
   on er.external_resource_id = dcr.orion_id
  WHERE regexp_extract(dcr.shopper_id,'[a-zA-Z]+', 0) = '' 
 group by dcr.shopper_id, er.resource_id, case when dcr.e_id = 'hosting.cpanel.api.customer_migration.complete' then 'Migrate' when dcr.e_id = 'hosting.cpanel.account.setup.complete' then 'Setup' else 'Publish' end, 'cPanel'
),

dcr_union as
(

select dcr_cpnl.shopper_id, dcr_cpnl.resource_id, dcr_cpnl.prod_type, to_date(dcr_cpnl.event_date) as event_date, dcr_cpnl.event_action
from dcr_cpnl
)

insert overwrite table prod_events
select dcr_union.shopper_id, dcr_union.resource_id, dcr_union.prod_type, to_date(dcr_union.event_date) as event_date, dcr_union.event_action
from dcr_union
group by dcr_union.shopper_id, dcr_union.resource_id, dcr_union.prod_type, dcr_union.event_date, dcr_union.event_action
;