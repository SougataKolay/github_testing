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
    SELECT 
        timeexpdate,
        yearnum,
        monthnum,
        empnum,
        newautopay
    FROM member_services_mart.mbr_auto_renewal_fsu_history

    UNION ALL

    SELECT 
        CAST(timeexpdate AS DATE),
        CAST(yearnum AS INT),
        CAST(monthnum AS INT),
        CAST(empnum AS INT),
        CAST(newautopay AS INT)
    FROM mbr_auto_renewal_curr_mth_snapshot
) t

WHERE TimeExpDate >= add_months(current_date, -12)