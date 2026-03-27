select 
10 as metric_id
,TimeExpDate
, EMPNUM 
,NULL as c1
,'' as c2
,'' as c3
,NewAutoPay
,NULL as f2
,NULL as f3
,NULL as f4
,NULL as f5
,NULL as f6
,'' as c4
,'' as c5
,'' as c6
,'' as c10
,'' as c11
,'' as c12
from (
select * from member_services_mart.mbr_auto_renewal_fsu_history
union
select cast(timeexpdate as Date) as timeexpdate, cast(yearnum as int) yearnum,cast(monthnum as int) as monthnum,cast(empnum as int) as empnum,cast(newautopay as int) as newautopay from mbr_auto_renewal_curr_mth_snapshot
) WHERE YEAR(current_date) - YearNum <=1