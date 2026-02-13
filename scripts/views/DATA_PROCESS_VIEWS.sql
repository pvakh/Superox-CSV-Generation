
  CREATE OR REPLACE FORCE EDITIONABLE VIEW "SUPEROX"."DATA_PROCESS_VIEWS" ("PROCESS_ID", "VERSION_ID", "PROCESS_RESULT_VIEW_NAME", "PROCESS_LOG_FORMATTED", "PROCESS_LOG_FLINK_FORMATTED", "INSERT_STATEMENT", "INSERT_STATEMENT_FLINK", "PROCESS_RESULT", "PROCESS_RESULT_SUBSET_NAME", "PROCESS_RESULT_SUBSET", "CSV_HEADERS", "QUERY_TO_CSV") AS 
  SELECT process_id,
    version_id,
    process_name
    ||'_RESULT_V'
    ||version_id Process_result_view_name,
    'Create or replace view '
    ||process_name
    ||'_LOG_V'
    ||version_id
    ||' as select * from ( select process_id, ts_REQUEST_TIME, max(max_rec_id) over (PARTITION BY ts_REQUEST_TIME) max_rec_id, process_var, var_value from log_upload_formatted where process_id = '
    ||process_id
    ||') pivot ( max(var_value) for process_var in ('
    ||pivot_clause
    ||'))' Process_log_formatted,
    'Create or replace view '
    ||process_name
    ||'_LOG_FL'
    ||version_id
    ||' as select * from ( select process_id, ts_REQUEST_TIME, batch_ID, MAX(rec_id) over (PARTITION BY batch_id) max_rec_id, substr(var_name, instr(var_name,''.'',-1,1)+1) process_var, var_value from log_upload_FLINK_BUFFER where process_id = '
    ||process_id
    ||') pivot ( max(var_value) for process_var in ('
    ||pivot_clause
    ||'))' Process_log_FLINK_formatted,
    'Insert into process_results_temp (Process_id, REQUEST_TS, Version_id, tape_id, tape_name, run_id, global_run_id, position, Process_id_rep, reel_start, reel_end, '
    || insert_fields
    ||' , Create_date, max_rec_id) (Select Process_id, ts_request_time REQUEST_TS, '
    ||Version_id
    ||' Version_id, Null tape_id, Upper(replace (tape_name,'' '', null)) tape_name, run_id, null global_run_id, position, Process_id_rep, reel_start, reel_end, '
    || insert_fields
    ||' ,sysdate, max_rec_id From '
    ||process_name
    ||'_LOG_V'
    ||version_id
    ||')' Insert_statement,
    'Insert into process_results_FLINK_temp (Process_id, REQUEST_TS, BATCH_ID,Version_id, tape_id, tape_name, run_id, global_run_id, position, Process_id_rep, reel_start, reel_end, '
    || insert_fields
    ||' , Create_date, max_rec_id) (Select Process_id, ts_request_time REQUEST_TS, batch_ID, '
    ||Version_id
    ||' Version_id, Null tape_id, Upper(replace (tape_name,'' '', null)) tape_name, run_id, null global_run_id, position, Process_id_rep, reel_start, reel_end, '
    || insert_fields
    ||' ,sysdate, max_rec_id From '
    ||process_name
    ||'_LOG_FL'
    ||version_id
    ||')' Insert_statement_FLINK,
    'Create or replace view '
    ||process_name
    ||'_RESULT_V'
    ||version_id
    ||' as select PROCESS_ID, REQUEST_TS, BATCH_ID, VERSION_ID, TAPE_ID, TAPE_NAME, RUN_ID, GLOBAL_RUN_ID, POSITION, REEL_START, REEL_END, '
    ||list_result_clause
    ||' from Process_results where process_id = '
    ||process_id
    ||' and version_id = '
    ||version_id Process_Result,
    process_name||'_RESULT_S' Process_result_subset_name,
    'Create or replace view '
    ||process_name
    ||'_RESULT_S'
    ||' as select PROCESS_ID, REQUEST_TS, BATCH_ID, VERSION_ID, TAPE_ID, TAPE_NAME, RUN_ID, GLOBAL_RUN_ID, POSITION, REEL_START, REEL_END, '
    ||list_result_clause
    ||' from Process_results_subset where process_id = '
    ||process_id
    ||' and version_id = '
    ||version_id Process_Result_subset,
    'PROCESS_ID'
    ||CHR(9)
    ||'REQUEST_TS'
    ||CHR(9)
    ||'VERSION_ID'
    ||CHR(9)
    ||'TAPE_ID'
    ||CHR(9)
    ||'TAPE_NAME'
    ||CHR(9)
    ||'RUN_ID'
    ||CHR(9)
    ||'GLOBAL_RUN_ID'
    ||CHR(9)
    ||'POSITION'
    ||CHR(9)
    ||'REEL_START'
    ||CHR(9)
    ||'REEL_END'
    ||CHR(9)
    ||result_fields csv_headers,
    'Select PROCESS_ID||chr(9)||to_char(REQUEST_TS,''DD/MM/YYYY HH24:MI:SS.FF2'')||chr(9)||VERSION_ID||chr(9)||TAPE_ID||chr(9)||TAPE_NAME||chr(9)||RUN_ID||chr(9)||GLOBAL_RUN_ID||chr(9)||POSITION||chr(9)||REEL_START||chr(9)||REEL_END||chr(9)||'
    ||results_in_csv
    ||' from Process_results where process_id = '
    ||process_id
    ||' and version_id = '
    ||version_id Query_to_csv
  FROM
    (SELECT p.process_id,
      pvv.version_id,
      p.process_name,
      (SELECT listagg (''''
        ||variable_short_name
        ||''' as '
        ||result_column,',') within GROUP (
      ORDER BY column_order)
      FROM process_variables pv
      WHERE pv.process_id = p.process_id
      AND pv.version_id   = pvv.version_id
      ) pivot_clause,
      (SELECT listagg (result_column,',') within GROUP (
      ORDER BY column_order)
      FROM process_variables pv
      WHERE pv.process_id = p.process_id
      AND pv.version_id   = pvv.version_id
      AND pv.ignore       =2
      ) Insert_fields,
      (SELECT listagg (result_column
        ||' as '
        ||variable_name,',') within GROUP (
      ORDER BY column_order)
      FROM process_variables pv
      WHERE pv.process_id = p.process_id
      AND pv.version_id   = pvv.version_id
      AND pv.ignore       =2
      ) list_result_clause,
      (SELECT listagg (variable_name,chr(9)) within GROUP (
      ORDER BY column_order)
      FROM process_variables pv
      WHERE pv.process_id = p.process_id
      AND pv.version_id   = pvv.version_id
      AND pv.ignore       =2
      ) result_fields,
      (SELECT listagg (result_column,'||chr(9)||') within GROUP (
      ORDER BY column_order)
      FROM process_variables pv
      WHERE pv.process_id = p.process_id
      AND pv.version_id   = pvv.version_id
      AND pv.ignore       =2
      ) results_in_csv
    FROM processes p,
      process_variable_versions pvv
    WHERE p.process_id = pvv.process_Id
    AND p.log_type     ='FLINK'
    )a;
