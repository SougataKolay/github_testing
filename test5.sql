-- revised query 
with mbr_auto_renewal as (
SELECT
AR.CLUB_CODE,
AR.MEMBER_NUM,
AR.BILL_PLAN_C_OLD,
AR.BILL_PLAN_X_OLD,
AR.BILL_PLAN_C_NEW,
AR.BILL_PLAN_X_NEW,
AR.RPT_TXN_T AS TRANSACTION_TIME,
AR.PAYMENT_T AS PAYMENT_TIME,
AR.TRANSACTION_TYPE,
AR.CLUB_JOIN_YEAR,
cast(CASE WHEN AR.EMPLOYEE_ID = 0 AND AR.AUTH_EMPLOYEE_ID = 999999 THEN 9999
                       WHEN AR.EMPLOYEE_ID = 0 AND AR.AUTH_EMPLOYEE_ID > 0 AND AR.AUTH_EMPLOYEE_ID <> 999999  THEN AR.AUTH_EMPLOYEE_ID
                                                      ELSE AR.EMPLOYEE_ID
           END AS int) AS EMPLOYEE_ID,
cast(CASE WHEN AR.EMPLOYEE_ID = 0 AND AR.AUTH_EMPLOYEE_ID = 999999 THEN 36 ELSE AR.BRANCH_NUMBER END AS int) AS BRANCH_NUMBER,
cast(CASE WHEN AR.EMPLOYEE_ID = 0 AND AR.AUTH_EMPLOYEE_ID = 999999 THEN '36' ELSE AR.EMPLOYEE_SECTION END AS string)  AS EMPLOYEE_SECTION,
AR.MEMBERSHIP_EFF_DATE,
AR.ROLE_CODE,
AR.CUSTOMER_ID,
AR.AUTH_DATE as AUTH_DATE,
AR.AUTH_SOURCE,
AR.AUTH_EMPLOYEE_ID,
AR.REGION_NUMBER,
(CASE WHEN AR.AUTH_SOURCE = 'GF' THEN 'T'
             WHEN AR.AUTH_DATE IS NOT NULL AND datediff(cast(AR.AUTH_DATE as date), cast(AR.TRANSACTION_TIME as date)) < 31 THEN 'T'
             WHEN AR.AUTH_DATE IS NOT NULL AND datediff(cast(AR.AUTH_DATE as date), cast(AR.TRANSACTION_TIME as date)) > 30 THEN 'N'
             ELSE 'F'
END) AS AUTH_FLAG,          
(CASE WHEN AR.AUTH_SOURCE = 'GF' THEN 'T'
            WHEN AR.AUTH_DATE IS NOT NULL AND datediff(cast(AR.AUTH_DATE as date), cast(AR.TRANSACTION_TIME as date)) < 61 THEN 'T'
            ELSE 'F'
END) AS AUTH_SIXTY_FLAG,                      
(CASE WHEN AR.TRANSACTION_TYPE = 'NEW' THEN 'T' ELSE 'F' END) AS NEW_MBR_FLAG,   
(CASE WHEN TRIM(AR.TRANSACTION_TYPE) = 'NEW' AND  TRIM(AR.BILL_PLAN_C_NEW) IN ('AC', 'AH','MP')  THEN 0   
            WHEN TRIM(AR.TRANSACTION_TYPE) <> 'NEW' AND  TRIM(AR.BILL_PLAN_C_NEW) IN ('AC', 'AH') AND datediff(cast(AR.TRANSACTION_TIME as date), AR.MEMBERSHIP_EFF_DATE) <= 0 THEN 0
            WHEN TRIM(AR.TRANSACTION_TYPE) <> 'NEW' AND  TRIM(AR.BILL_PLAN_C_NEW) IN ('AC', 'AH') AND datediff(cast(AR.TRANSACTION_TIME as date), AR.MEMBERSHIP_EFF_DATE) > 0 AND cast(AR.TRANSACTION_TIME AS DATE) <= add_months(AR.MEMBERSHIP_EFF_DATE,12) THEN 1
            WHEN TRIM(AR.TRANSACTION_TYPE) <> 'NEW' AND  TRIM(AR.BILL_PLAN_C_NEW) IN ('AC', 'AH') AND datediff(cast(AR.TRANSACTION_TIME as date), AR.MEMBERSHIP_EFF_DATE) > 0 AND cast(AR.TRANSACTION_TIME AS DATE) > add_months(AR.MEMBERSHIP_EFF_DATE,12) AND cast(AR.TRANSACTION_TIME AS DATE) <= add_months(AR.MEMBERSHIP_EFF_DATE,24) THEN 2
            WHEN TRIM(AR.TRANSACTION_TYPE) <> 'NEW' AND  TRIM(AR.BILL_PLAN_C_NEW) IN ('AC', 'AH') AND datediff(cast(AR.TRANSACTION_TIME as date), AR.MEMBERSHIP_EFF_DATE) > 0 AND cast(AR.TRANSACTION_TIME AS DATE) > add_months(AR.MEMBERSHIP_EFF_DATE,24) THEN 3
            ELSE NULL          
END) AS TENURE_N,
ar.member_name,
ar.member_tel_no,
(CASE WHEN AR.EMPLOYEE_ID IN (9999,999999,888888,490989)  THEN 'T'
                       WHEN AR.EMPLOYEE_ID = 0 AND AR.AUTH_EMPLOYEE_ID IN (9999,999999,888888,490989)  THEN 'T'
                       ELSE 'F'
END)  AS INTERNET_FLAG,  
AR.EXTRACT_T,
(CASE WHEN AR.AUTH_DATE IS NULL THEN NULL
            WHEN AR.AUTH_DATE > cast(AR.PAYMENT_T AS DATE) THEN AR.AUTH_DATE
                                           WHEN AR.AUTH_DATE < cast(AR.PAYMENT_T AS DATE) THEN cast(AR.PAYMENT_T AS DATE)
                                           ELSE AR.AUTH_DATE END) AS REPORTING_DATE
FROM glue_catalog.member_services_mart.membership_auto_renewal AR),
TIME_MONTH as (
SELECT
    TMH_C AS TimeCode,
    TMH_EFF_D AS TimeEffDate,
    TMH_EXP_D AS TimeExpDate,
    TMH_YEAR_C AS TimeYearCode,
    TMH_YEAR_EFF_D AS TimeYearEffDate,
    TMH_YEAR_EXP_D AS TimeYearExpDate,
    TMH_QTR_C AS TimeQtrCode,
    TMH_QTR_EFF_D AS TimeQtrEffDate,
    TMH_QTR_EXP_D AS TimeQtrExpDate,
    TMH_MONTH_N AS TimeMonthNumber,
    TMH_QTR_N AS TimeQuarterNumber,
    TMH_MONTH_ABBR_X AS TimeMonthAbbr,
    TMH_QTR_ABBR_X AS TimeQuarterAbbr
FROM glue_catalog.ace_common.DW_TIME_MONTH_HIST
WHERE TMH_C <> 210012
  AND TMH_EFF_D > date_add(date_add(date_trunc('year', current_date()), -365*5), -1)
),
/* ---------------- FIRST BLOCK (PAYMENT BASED) ---------------- */
PAYMENT_BASED AS (
    SELECT 
        a.TimeEffDate,
        a.TimeExpDate,
        a.AUTH_DATE,
        a.transaction_date,
        a.payment_date,
        CAST(a.employee_id AS string) AS EMPLOYEENUMBER,
        a.MEMBER_NUM, 
        a.CLUB_CODE,
        a.ROLE_CODE,
        SUM(a.OneYr + a.TwoYr + a.ThreePlus) AS AUTO_PAY_COUNT
    FROM (
        SELECT 
            b.TimeEffDate,
            b.TimeExpDate,
            CAST(a.transaction_time AS DATE) AS transaction_date, 
            a.auth_date,
            CAST(a.payment_time AS DATE) AS payment_date,
            a.Employee_id,
            a.MEMBER_NUM,
            a.Club_Code,
            a.Role_Code,
            SUM(CASE WHEN a.tenure_n = 0 THEN 1 ELSE 0 END) AS ZeroYr,
            SUM(CASE WHEN a.tenure_n = 1 THEN 1 ELSE 0 END) AS OneYr,
            SUM(CASE WHEN a.tenure_n = 2 THEN 1 ELSE 0 END) AS TwoYr,
            SUM(CASE WHEN a.tenure_n = 3 THEN 1 ELSE 0 END) AS ThreePlus
        FROM mbr_auto_renewal a
        INNER JOIN (
            SELECT
                MAX(TRANSACTION_TIME) AS Max_TRAN_TIME, 
                t.TimeEffDate,
                t.TimeExpDate,
                MEMBER_NUM,
                Club_Code,
                Role_Code 
            FROM mbr_auto_renewal
            INNER JOIN TIME_MONTH t
              ON CAST(payment_time AS DATE) BETWEEN t.TimeEffDate AND t.TimeExpDate
            WHERE CAST(TRANSACTION_TIME AS DATE) >= date_add(t.TimeEffDate, -60)
              AND CAST(TRANSACTION_TIME AS DATE) <= t.TimeExpDate
            GROUP BY 2,3,4,5,6
        ) b
        ON b.Max_TRAN_TIME = a.TRANSACTION_TIME 
        AND a.CLUB_CODE = b.Club_Code 
        AND a.MEMBER_NUM = b.MEMBER_NUM 
        AND a.Role_Code = b.Role_Code
        WHERE a.bill_plan_c_new IN ('AC','AH','MP')
          AND a.AUTH_FLAG = 'T'
        GROUP BY 1,2,3,4,5,6,7,8,9
    ) a
    WHERE year(current_date()) - year(CAST(a.AUTH_DATE AS DATE)) <= 1
    GROUP BY 1,2,3,4,5,6,7,8,9
),

/* ---------------- SECOND BLOCK (AUTH BASED) ---------------- */
AUTH_BASED AS (
    SELECT 
        a.TimeEffDate,
        a.TimeExpDate,
        a.AUTH_DATE,
        a.transaction_date,
        a.payment_date,
        CAST(a.employee_id AS string) AS EMPLOYEENUMBER,
        a.MEMBER_NUM, 
        a.CLUB_CODE,
        a.ROLE_CODE,
        SUM(a.OneYr + a.TwoYr + a.ThreePlus) AS AUTO_PAY_COUNT
    FROM (
        SELECT 
            b.TimeEffDate,
            b.TimeExpDate,
            CAST(a.transaction_time AS DATE) AS transaction_date, 
            a.auth_date,
            CAST(a.payment_time AS DATE) AS payment_date,
            a.Employee_id,
            a.MEMBER_NUM,
            a.Club_Code,
            a.Role_Code,
            SUM(CASE WHEN a.tenure_n = 1 THEN 1 ELSE 0 END) AS OneYr,
            SUM(CASE WHEN a.tenure_n = 2 THEN 1 ELSE 0 END) AS TwoYr,
            SUM(CASE WHEN a.tenure_n = 3 THEN 1 ELSE 0 END) AS ThreePlus
        FROM mbr_auto_renewal a
        INNER JOIN (
            SELECT
                MAX(TRANSACTION_TIME) AS Max_TRAN_TIME, 
                t.TimeEffDate,
                t.TimeExpDate,
                MEMBER_NUM,
                Club_Code,
                Role_Code
            FROM mbr_auto_renewal
            INNER JOIN TIME_MONTH t
              ON AUTH_DATE BETWEEN t.TimeEffDate AND t.TimeExpDate
            WHERE CAST(TRANSACTION_TIME AS DATE) >= date_add(t.TimeEffDate, -60)
              AND CAST(TRANSACTION_TIME AS DATE) <= t.TimeExpDate
            GROUP BY 2,3,4,5,6
        ) b
        ON b.Max_TRAN_TIME = a.TRANSACTION_TIME 
        AND a.CLUB_CODE = b.Club_Code 
        AND a.MEMBER_NUM = b.MEMBER_NUM 
        AND a.Role_Code = b.Role_Code
        WHERE a.bill_plan_c_new IN ('AC','AH','MP')
          AND a.AUTH_FLAG = 'T'
        GROUP BY 1,2,3,4,5,6,7,8,9
    ) a
    WHERE a.payment_date < a.TimeEffDate
      AND year(current_date()) - year(CAST(a.AUTH_DATE AS DATE)) <= 1
    GROUP BY 1,2,3,4,5,6,7,8,9
)
/* ---------------- FINAL UNION ---------------- */
Select 
11 as metric_id
,TIMEEXPDATE
,cast(EMPLOYEENUMBER AS INT) as EMPLOYEENUMBER
,NULL as branch_n_orig
,'' as c1
,'' as c2
,AUTO_PAY_COUNT                      
,NULL as fact2
,NULL as fact3
,NULL as fact4
,NULL as fact5
,NULL as fact6
,concat('ClubNo:',Club_Code) as Club_Code
,concat('TransactionDate :',cast(Transaction_Date as string)) as Transaction_Date
,concat('Rolecode :',Role_Code) as Role_Code
,concat('MemberNo :',cast(Member_Num as string)) as Member_Num
,concat('AuthDate :',cast(Auth_Date as string)) as Auth_Date
,concat('PaymentDate :',cast(Payment_Date as string)) as Payment_Date from (
SELECT * FROM PAYMENT_BASED
UNION
SELECT * FROM AUTH_BASED);