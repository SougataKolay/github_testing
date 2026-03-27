-- utc_timezone
SELECT
     8 as Metric_ID
   , CAST(substr(call_start_time_utc, 1, 10) AS DATE) AS transaction_date
   , CAST(A.employee_id AS INT)
   , null AS Branch_N_Orig
   , SUM(CASE 
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
   , null AS Fact2
   , null AS Fact4
   , null AS Fact5
   , null AS Fact6
   , concat('CallType:', virtual_queue) AS Dimension1
   , concat('CallDirection:', interaction_type) AS Dimension2
   , '' AS Dimension3
   , '' AS Dimension4
   , '' AS Dimension5
   , '' AS Dimension6
 fROM enterprise_call_segment_master A
INNER JOIN hr_master.employee_history_master staff ON (staff.employee_id = CAST(A.employee_id AS INT)) AND CAST(substr(A.call_start_time_utc, 1, 10) AS DATE) BETWEEN staff.record_effective_date AND staff.record_expiration_date
INNER JOIN ace_common.ace_mapping_raw_new xref ON (staff.housed_center = xref.HRCHARGESECTION) AND CAST(substr(A.call_start_time_utc, 1, 10) AS DATE) BETWEEN xref.EFF_T AND xref.EXP_T AND ((xref.TYPE_X = 'FSU') OR ((xref.TYPE_X = 'Branch') AND (xref.BUSINESS_UNIT IN ('B743', 'B862'))))
WHERE (YEAR(current_date()) - YEAR(CAST(substr(A.call_start_time_utc, 1, 10) AS DATE))) <= 1 AND (xref.club_x IN ('Texas', 'New Mexico')) AND ((A.virtual_queue LIKE 'VQ_ISS_%_TX') OR (A.virtual_queue LIKE 'VQ_MSC_%_TX') OR (A.virtual_queue LIKE 'VQ_MSC_%_NM') OR (A.virtual_queue LIKE 'VQ_ISS_%_NM')) AND upper(trim(A.interaction_type)) in ('INBOUND', 'OUTBOUND', 'INTERNAL') AND A.data_source = 'GIM' AND trim(upper(resource_type)) = 'AGENT'
GROUP BY A.employee_id, A.virtual_queue, A.interaction_type, substr(A.call_start_time_utc, 1, 10)