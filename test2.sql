mbr_auto_renewal_curr_mth_snapshot as (

SELECT 

extract(year from date_trunc('month', current_date)) AS YearNum,
extract(month from date_trunc('month', current_date)) AS MonthNum,
date_add(
        'day', 
        -1, 
        date_add('month', 1, date_trunc('month', current_date))
    ) AS TimeExpDate,

 a.Employee_id AS EmpNum,
 SUM(CASE WHEN a.tenure_n= 0 THEN 1 ELSE 0 end ) AS NewAutoPay
FROM MBR_Auto_Renewal a
INNER JOIN 
(SELECT
MAX(MBR_Auto_Renewal.TRANSACTION_TIME) AS Max_TRAN_TIME, MBR_Auto_Renewal.MEMBER_NUM,
MBR_Auto_Renewal.Club_Code,
MBR_Auto_Renewal.Role_Code
FROM   MBR_Auto_Renewal MBR_Auto_Renewal
WHERE
date(MBR_Auto_Renewal.TRANSACTION_TIME) >= date_add(
    'day',
    -60,
    date_trunc('month', current_date)
)
AND date(MBR_Auto_Renewal.TRANSACTION_TIME) <= date_add(
        'day', 
        -1, 
        date_add('month', 1, date_trunc('month', current_date))
    )
GROUP BY
MBR_Auto_Renewal.MEMBER_NUM,
MBR_Auto_Renewal.Club_Code,
MBR_Auto_Renewal.Role_Code) b
ON b.Max_TRAN_TIME=a.TRANSACTION_TIME AND  a.CLUB_CODE = b.Club_Code AND a.MEMBER_NUM = b.MEMBER_NUM AND a.Role_Code = b.Role_Code
 WHERE a.bill_plan_c_new IN ('AC','AH','MP')
AND 
(
                (
    date(a.payment_time) >= date_trunc('month', current_date)
    AND date(a.payment_time) <= date_add(
        'day', 
        -1, 
        date_add('month', 1, date_trunc('month', current_date))
    )
)

OR
(
    date(a.payment_time) < date_trunc('month', current_date)
    AND a.auth_date >= date_trunc('month', current_date)
    AND a.auth_date <= date_add(
        'day',
        -1,
        date_trunc('month', current_date)
    )
)
)
AND a.AUTH_FLAG = 'T'
GROUP BY 1,2,3,4)