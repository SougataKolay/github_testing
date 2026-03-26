convert below query in to spark 
 

-- utc_timezone
SELECT
     8 as Metric_ID
   , TRY_CAST(substr(call_start_time_utc,1,10) as date) transaction_date
    , CAST(A.employee_id AS INTEGER)
   , null Branch_N_Orig
   ,Sum(CASE 
             WHEN upper(trim(resource_role)) IN 
                  ( 
                         'ROUTEDTO', 'RECEIVEDTRANSFER', 'RECEIVEDCONSULT', 
                         'RECEIVED', 
                                                               'INCONFERENCE', 
                         'DIVERTEDTO' 
                  ) 
                  AND upper(trim(technical_result)) NOT IN 
                      ( 
                      'ABANDONED', 'REDIRECTED', 'CLEARED', 'PULLED', 
                      'DESTINATIONBUSY' 
                      ) 
                  AND upper(trim(result_reason)) <> 
                      'ABANDONEDWHILERINGING' 
                  AND upper(trim(resource_type)) = 'AGENT' THEN 1 
             ELSE 0 
           END) AS Total_Calls_Answered
     , null Fact2
   , null Fact4
   , null Fact5
   , null Fact6
   , concat('CallType:', virtual_queue) Dimension1
   , concat('CallDirection:', interaction_type) Dimension2
   , '' Dimension3
   , '' Dimension4
   , '' Dimension5
   , '' Dimension6
  from ((enterprise_call_segment_master A
INNER JOIN hr_master.employee_history_master staff ON ((staff.employee_id = TRY_CAST(A.employee_id AS INT)) AND (TRY_CAST(substr(A.call_start_time_utc, 1, 10) AS DATE) BETWEEN staff.record_effective_date AND staff.record_expiration_date)))
   INNER JOIN ace_common.ace_mapping_raw_new xref ON ((staff.housed_center = xref.HRCHARGESECTION) AND (TRY_CAST(substr(A.call_start_time_utc, 1, 10) AS DATE) BETWEEN xref.EFF_T AND xref.EXP_T) AND ((xref.TYPE_X = 'FSU') OR ((xref.TYPE_X = 'Branch') AND (xref.BUSINESS_UNIT IN ('B743', 'B862'))))))
   WHERE (((EXTRACT(YEAR FROM current_date) - EXTRACT(YEAR FROM TRY_CAST(substr(A.call_start_time_utc, 1, 10) AS date))) <= 1) AND (xref.club_x IN ('Texas', 'New Mexico')) AND ((A.virtual_queue LIKE 'VQ_ISS_%_TX') OR (A.virtual_queue LIKE 'VQ_MSC_%_TX') OR (A.virtual_queue LIKE 'VQ_MSC_%_NM') OR (A.virtual_queue LIKE 'VQ_ISS_%_NM'))) AND upper(trim(A.interaction_type)) in ('INBOUND','OUTBOUND','INTERNAL') and A.data_source = 'GIM' AND trim(upper(resource_type)) ='AGENT' 
   GROUP BY A.employee_id, A.virtual_queue, A.interaction_type, substr(A.call_start_time_utc, 1, 10)