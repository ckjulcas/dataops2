INSERT INTO
   demo.ops_event_log (
       run_id,
       event_ts,
       stage,
       status,
       details
   )
VALUES (
       '${RUN_ID}',
       current_timestamp(),
       '${STAGE}',
       '${STATUS}',
       '${DETAILS}'
   );
