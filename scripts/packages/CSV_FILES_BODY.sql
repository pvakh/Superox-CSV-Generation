create or replace PACKAGE BODY CSV_FILES AS

PROCEDURE initial_job  is

Cursor global_run_cur is 
SELECT tpr.*,
  rj.record_count cnt
FROM tape_process_run tpr,
  run_journal rj
WHERE tpr.global_run_id = rj.global_run_id
AND NVL(tpr.file_record_count,0)-rj.record_count<-10
AND Substr(NVL(tpr.csv_file_name,'X'),1,3) <> 'ORA'
and tpr.tape_id >1
AND rj.end_time BETWEEN Sysdate-7 AND sysdate-15/1440;

-- NEW 2/13/26: Cursor for Run-level files (Consolidating all tapes for a Run)
    CURSOR master_run_cur IS
SELECT DISTINCT tpr.process_id, tpr.run_num
FROM tape_process_run tpr,
  run_journal rj
WHERE tpr.global_run_id = rj.global_run_id
AND NVL(tpr.RUN_record_count,0)-rj.record_count<-10
AND Substr(NVL(tpr.csv_RUN_FILE_name,'X'),1,3) <> 'ORA'
and tpr.tape_id >1
AND rj.end_time BETWEEN Sysdate-7 AND sysdate-15/1440
AND rj.process_id IN (520, 521, 220); -- <--- ADD YOUR SPECIFIC PROCESS_IDs HERE


Begin
  for rec in global_run_cur LOOP
      csv_files.write_file(rec.global_run_id); 
  END LOOP;

  FOR m_rec IN master_run_cur LOOP
        csv_files.write_run_file(m_rec.process_id, m_rec.run_num);
  END LOOP;

End initial_job ;

PROCEDURE Delete_job  is


Cursor global_run_cur is 
  Select tpr.global_run_id
  From tape_process_run tpr,
       run_journal rj
  Where tpr.global_run_id = rj.global_run_id
    and rj.end_time <Sysdate-181;

 vFile_name varchar2(200);
 vFile_name1 varchar2(200);

Begin
  for rec in global_run_cur LOOP
  err_loc:='E_16';
  Select tpr.csv_file_name, tpr.CSV_RUN_FILE_NAME
  Into vFile_name, vFile_name1
  from tape_process_run tpr
  where tpr.global_run_id = rec.global_run_id;

     begin
      err_loc:='E_17';
         UTL_FILE.FREMOVE ('CSV_DIR', vFile_name);
     exception
        When Others then
             NULL;
     end;        
     begin
      err_loc:='E_17';
         UTL_FILE.FREMOVE ('CSV_DIR', vFile_name1);
     exception
        When Others then
             NULL;
     end;        

  err_loc:='E_18';
  Update tape_process_run
  set csv_file_name = null,
      file_record_count = null,
      file_created_date = null,
      csv_run_file_name = null,
      run_record_count = null,
      run_file_created_date = null
  Where global_run_id = rec.global_run_id;

 commit;

  END LOOP;

End delete_job ;

procedure write_file (pGlobal_run_id Number) is
  file_handle UTL_FILE.file_type;
  
  TYPE CSVCurTyp IS REF CURSOR;  
  CSV_CUR   CSVCurTyp;  
  
  FetchedRow varchar2(4000);
  
  vFile_name varchar2(50);
  vRecord_count number;
  vCSV_headers varchar2(4000);
  vQuery varchar2(4000);
  Columns_as_text varchar2(4000);
  
  minTS varchar2(30);
  maxTS varchar2(30);
  
  vProcess_id number;
  sql_err Varchar2(200);
  
  vTtable_name varchar2(30);
begin
       
     
  err_loc:='E_4';
  select process_id 
    into vProcess_id
    from tape_process_run tpr
  where tpr.global_run_id = pGlobal_run_id;
  
IF vProcess_id not in (610,3012) then 


  err_loc:='E_1';
  Select 'P'||tpr.Process_id||'-R'||tpr.Run_NUM||'-'||
  (Select replace(replace(tape_name,'#','X'),',','.') from tape_inventory ti where ti.tape_id = tpr.tape_id)
  ||'.tsv'
  Into vFile_name
  from tape_process_run tpr
  where tpr.global_run_id = pGlobal_run_id;
  err_loc:='E_2';
--Delete old file just in case.
     begin
         UTL_FILE.FREMOVE ('CSV_DIR', vFile_name);
     exception
        When Others then
        err_loc:='E_3';
             NULL;
     end; 




  err_loc:='E_5';
  file_handle := utl_file.fopen('CSV_DIR', vFile_name , 'w', 32767);
  err_loc:='E_6';
  select csv_headers, query_to_csv||' and global_run_id ='||pGlobal_Run_ID||' order by position'
  into vCSV_headers, vQuery
  from data_process_views dv,
      Tape_process_run tpr
  where tpr.process_id = dv.Process_id
    and version_id = (select max(version_id) from process_results pr where pr.global_run_id = tpr.global_run_id)
    and tpr.global_run_id = pGlobal_run_id;
  err_loc:='E_7';
   utl_file.put_line(file_handle, vCSV_headers);
  --dbms_output.put_line (vQuery);  
  open CSV_CUR for vQuery;
  
  LOOP
      FETCH CSV_CUR INTO FetchedRow; --fetch a row in it
        EXIT WHEN CSV_CUR%NOTFOUND;
        err_loc:='E_8';
         utl_file.put_line(file_handle, FetchedRow);
         vRecord_count:=csv_cur%rowcount;
      
         --dbms_output.put_line(fetchedrow);       
   END LOOP;
CLOSE CSV_CUR;
    err_loc:='E_9';
  utl_file.fclose(file_handle);



 ELSE
  IF vProcess_id in (610,3012) Then
     
     err_loc:='E_10';
    For names in 
      (select process_id, csv_headers, query_to_csv||' and global_run_id ='||pGlobal_Run_ID||' order by position' query_text
   --   select process_id, process_result_view_name t_name, query_to_csv||' and global_run_id ='||pGlobal_Run_ID||' order by position' query_text
         from data_process_views
         where floor(process_id) = vProcess_id)
     LOOP

      err_loc:='E_1';
  Select 'P'||NAMES.Process_id||'-R'||tpr.Run_NUM||'-'||
  (Select replace(replace(tape_name,'#','X'),',','.') from tape_inventory ti where ti.tape_id = tpr.tape_id)
  ||'.tsv'
  Into vFile_name
  from tape_process_run tpr
  where tpr.global_run_id = pGlobal_run_id;

  err_loc:='E_2';
--Delete old file just in case.

 --dbms_output.put_line (vFile_name);
     begin
         UTL_FILE.FREMOVE ('CSV_DIR', vFile_name);
     exception
        When Others then
        err_loc:='E_3';
             NULL;
     end; 
      file_handle := utl_file.fopen('CSV_DIR', vFile_name , 'w', 32767);
 
  err_loc:='E_12';
  
--  dump_table_to_CSV(names.t_name||'_temp','CSV_DIR',vFile_name,vRecord_count);
   utl_file.put_line(file_handle, names.CSV_headers);
--   dbms_output.put_line (names.Query_text);  
  open CSV_CUR for names.Query_text;
  
  LOOP
      FETCH CSV_CUR INTO FetchedRow; --fetch a row in it
        EXIT WHEN CSV_CUR%NOTFOUND;
           err_loc:='E_8';
         utl_file.put_line(file_handle, FetchedRow);
         vRecord_count:=csv_cur%rowcount;
      
         --dbms_output.put_line(fetchedrow);       
   END LOOP;
CLOSE CSV_CUR;
    err_loc:='E_9';
  utl_file.fclose(file_handle);
  
  
  
   END LOOP;
  END IF;
  
  
  
  
  END IF;  
  err_loc:='E_13';
  Update tape_process_run
  set csv_file_name = vFile_name,
      file_record_count = vRecord_count-1,
      file_created_date = sysdate
  Where global_run_id = pGlobal_run_id;
   err_loc:='E_14';
  INSERT into audit_log (event_ts, event, event_log)
   Values(SYSTIMESTAMP, 'CSV FILES CREATED', 'CSV FILE '||vFile_Name||' is done');
  commit;
  
  UTL_FILE.FCLOSE_ALL;
Exception 
      When others then  
   sql_err:=SUBSTR(SQLERRM, 1, 200);
      

   insert into Email_Alerts (alert, Subject, addrto, alert_text, addrfrom)
   values ('CSV FILE ERROR', 'Superox: CSV FILE ERROR', 'pvakh@vpinformatics.com',
   'CSV file generated error for global_run_id:'||pGlobal_run_id||'. Error:'||sql_err||' '||err_loc, 'pvakh@vpinformatics.com');
   
   sql_err:=SUBSTR(sql_err, 1, 99);
   err_loc:='E_15';
   Update tape_process_run
   set csv_file_name = sql_err
   Where global_run_id = pGlobal_run_id;
   
  UTL_FILE.FCLOSE_ALL; 
   
   commit;
   
   Raise;
   
end write_file;

PROCEDURE write_run_file (pProcess_id NUMBER, pRun_num NUMBER) IS
    file_handle UTL_FILE.file_type;
    vFile_name varchar2(100);
    vCSV_headers varchar2(4000);
    vQuery varchar2(4000);
    vFetchedRow varchar2(4000);
    vRecord_count  number;
    CSV_CUR sys_refcursor;
BEGIN
    -- Filename includes 'MASTER' so we don't overwrite tape files
    vFile_name := 'P'||pProcess_id||'-R'||pRun_num||'-MASTER_ALL_TAPES.tsv';

    -- Open the file
    file_handle := utl_file.fopen('CSV_DIR', vFile_name, 'w', 32767);

    -- Get the SQL Blueprint from your DATA_PROCESS_VIEWS
    -- We remove the 'global_run_id' filter and use 'run_id' instead
    SELECT csv_headers, 
           -- Note: We replace the specific global_run_id filter with run_id filter
           replace(query_to_csv, 'global_run_id =', 'run_id = '||pRun_num|| ' --')
    INTO vCSV_headers, vQuery
    FROM data_process_views
    WHERE process_id = pProcess_id
    AND version_id = (SELECT max(version_id) FROM process_results WHERE process_id = pProcess_id);

    -- Write Header
    utl_file.put_line(file_handle, vCSV_headers);

    -- Execute and Write Data
    OPEN CSV_CUR FOR vQuery;
    LOOP
        FETCH CSV_CUR INTO vFetchedRow;
        EXIT WHEN CSV_CUR%NOTFOUND;
        
        utl_file.put_line(file_handle, vFetchedRow);
        vRecord_count:=csv_cur%rowcount;
        
    END LOOP;
    CLOSE CSV_CUR;

    utl_file.fclose(file_handle);

 err_loc:='E_13';
  Update tape_process_run
  set csv_run_file_name = vFile_name,
      run_record_count = vRecord_count-1,
      run_file_created_date = sysdate
  Where process_id =pProcess_id
    and run_num=pRun_num;
   err_loc:='E_14';
  INSERT into audit_log (event_ts, event, event_log)
   Values(SYSTIMESTAMP, 'CSV RUN FILE CREATED', 'CSV RUN FILE '||vFile_Name||' is done');
  commit;
  
  UTL_FILE.FCLOSE_ALL;
Exception 
      When others then  
   sql_err:=SUBSTR(SQLERRM, 1, 200);
      

   insert into Email_Alerts (alert, Subject, addrto, alert_text, addrfrom)
   values ('CSV RUN FILE ERROR', 'Superox: CSV RUN FILE ERROR', 'pvakh@vpinformatics.com',
   'CSV RUN file generated error for run file :'||vFile_Name||'. Error:'||sql_err||' '||err_loc, 'pvakh@vpinformatics.com');
   
   sql_err:=SUBSTR(sql_err, 1, 99);
   err_loc:='E_15';
   Update tape_process_run
   set csv_run_file_name = sql_err
   Where process_id =pProcess_id
    and run_num=pRun_num;
   
  UTL_FILE.FCLOSE_ALL; 
   
   commit;
   
   Raise;
END write_run_file;

Procedure extract_IBAD_DATA is 
  vRecord_count number;
  sql_err Varchar2(200);
  vFile_name varchar2(50):='IBAD_EXTRACT_DATA_12_03_21.csv';
   vQuery varchar2(4000);
Begin
     
    For names in 
      (select process_result_view_name t_name
         from data_process_views
         where floor(process_id) = 610)
     LOOP
     vQuery:='insert into '||names.t_name||'_temp select * from '||names.t_name||' where request_TS> sysdate-14';
      
--      dbms_output.put_line (vQuery);
     Execute immediate vquery;  
     
     END LOOP;
  

  dump_table_to_CSV('IBAD_ALL_RESULTS_TEMP','CSV_DIR',vFile_name,vRecord_count);


  commit;
Exception 
      When others then  
   sql_err:=SUBSTR(SQLERRM, 1, 200);
      

   insert into Email_Alerts (alert, Subject, addrto, alert_text, addrfrom)
   values ('CSV FILE ERROR', 'Superox: CSV FILE ERROR', 'pvakh@vpinformatics.com',
   'CSV file generated error for IBAD DATA EXTRACT. Error:'||sql_err, 'pvakh@vpinformatics.com');
   
   commit;
   
End extract_IBAD_DATA;
 
 
  Procedure recreate_IBAD_TABLES is
  
cursor IBAD_LIST is
  select process_result_view_name t_name
  from data_process_views
  where floor(process_id) = 610;
  
  Query_text varchar2(1000);
    
  Begin
  
  for rec in ibad_list LOOP
  
  query_text:='Drop table '||rec.t_name||'_temp;';
 -- dbms_output.put_line (query_text);
  query_text:='Create global temporary table '||rec.t_name||'_temp as select * from '||rec.t_name||' where rownum<2;';
 -- dbms_output.put_line (query_text);
  
  End LOOP;
  err_loc:='E_19';
 for rec in ibad_list LOOP
  For col in ( 
   select table_name||'.'||column_name||' '||column_name||',' column_name
   from sys.all_tab_columns 
   where table_name = upper(rec.t_name )
   and owner = 'SUPEROX'
   order by column_id)
  LOOP
    query_text:=col.column_name;
    --dbms_output.put_line (query_text);
  End LOOP; 
 END LOOP;  
  
  End recreate_IBAD_TABLES;

END CSV_FILES;
