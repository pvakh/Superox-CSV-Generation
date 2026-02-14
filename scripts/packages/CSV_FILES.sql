create or replace PACKAGE CSV_FILES AS 

  PROCEDURE initial_job ;  
  PROCEDURE Delete_job ;  
  Procedure write_file (pGlobal_run_id Number);
  PROCEDURE write_run_file (pProcess_id NUMBER, pRun_num NUMBER);
  Procedure extract_IBAD_DATA;

  Procedure recreate_IBAD_TABLES;
    err_loc varchar2(5);
END CSV_FILES;
