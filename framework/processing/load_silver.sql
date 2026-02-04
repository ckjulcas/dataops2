INSERT INTO
   __SILVER_TABLE__ (
       ingest_ts,
       customer_id,
       customer_name,
       email,
       status,
       payload,
       dq_is_name_null,
       dq_is_underage,
       dq_status
   )
SELECT
   CAST(ingest_ts AS TIMESTAMP) AS ingest_ts,
   get_json_object (raw_payload, '$.customer_id') AS customer_id,
   NULLIF(
       TRIM(
           get_json_object (raw_payload, '$.name')
       ),
       ''
   ) AS customer_name,
   NULLIF(
       TRIM(
           get_json_object (raw_payload, '$.email')
       ),
       ''
   ) AS email,
   CASE
       WHEN NULLIF(
           TRIM(
               get_json_object (raw_payload, '$.name')
           ),
           ''
       ) IS NULL THEN 'FAIL_NAME_NULL'
       WHEN CAST(
           get_json_object (raw_payload, '$.age') AS INT
       ) < 18 THEN 'WARN_UNDERAGE'
       ELSE 'PASS'
   END AS status,
   raw_payload AS payload,
   CASE
       WHEN NULLIF(
           TRIM(
               get_json_object (raw_payload, '$.name')
           ),
           ''
       ) IS NULL THEN TRUE
       ELSE FALSE
   END AS dq_is_name_null,
   CASE
       WHEN CAST(
           get_json_object (raw_payload, '$.age') AS INT
       ) < 18 THEN TRUE
       ELSE FALSE
   END AS dq_is_underage,
   CASE
       WHEN NULLIF(
           TRIM(
               get_json_object (raw_payload, '$.name')
           ),
           ''
       ) IS NULL THEN 'FAIL_NAME_NULL'
       WHEN CAST(
           get_json_object (raw_payload, '$.age') AS INT
       ) < 18 THEN 'WARN_UNDERAGE'
       ELSE 'PASS'
   END AS dq_status
FROM demo.bronze_customers;



