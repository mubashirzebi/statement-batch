CREATE OR REPLACE PACKAGE FINACLE_BATCH_PKG AS

  PROCEDURE PREPARE_FILES(
    P_FILE_NAMES IN SYS.ODCIVARCHAR2LIST,
    P_OUT        OUT SYS_REFCURSOR
  );

  PROCEDURE FINALIZE_DB(
    P_RECS IN T_FINALIZE_TAB
  );

  PROCEDURE FINALIZE_MOVE(
    P_RECS IN T_MOVE_TAB
  );

END FINACLE_BATCH_PKG;
/

CREATE OR REPLACE PACKAGE BODY FINACLE_BATCH_PKG AS

  PROCEDURE PREPARE_FILES(
    P_FILE_NAMES IN SYS.ODCIVARCHAR2LIST,
    P_OUT        OUT SYS_REFCURSOR
  ) IS
    v_err_msg VARCHAR2(200);
  BEGIN
    MERGE INTO FINACLE_STMT_LOG t
    USING (
      SELECT DISTINCT COLUMN_VALUE AS FILE_NAME
      FROM TABLE(P_FILE_NAMES)
    ) s
    ON (t.FILE_NAME = s.FILE_NAME)
    WHEN NOT MATCHED THEN
      INSERT (
        CREATEDDATE, UPDATEDDATE, FILE_NAME, STATUS, DESCRIPTION
      )
      VALUES (
        SYSTIMESTAMP, SYSTIMESTAMP, s.FILE_NAME, 'RECEIVED', ''
      );

    UPDATE FINACLE_STMT_LOG
       SET DOCID = FIN_YEARLY_STMT_DOCID_SEQUENCE.NEXTVAL,
           UPDATEDDATE = SYSTIMESTAMP
     WHERE FILE_NAME IN (
             SELECT DISTINCT COLUMN_VALUE
             FROM TABLE(P_FILE_NAMES)
           )
       AND DOCID IS NULL;

    MERGE INTO FINACLE_STMT_LOG l
    USING (
      SELECT f.FILE_NAME,
             m.SOL_ID,
             m.CIFID,
             m.FORACID,
             CASE WHEN m.FILE_NAME IS NULL THEN 1 ELSE 0 END AS MISSING
        FROM (
          SELECT DISTINCT COLUMN_VALUE AS FILE_NAME
          FROM TABLE(P_FILE_NAMES)
        ) f
        LEFT JOIN (
          SELECT SOL_ID, CIFID, FORACID, ACCT_NAME, FILE_NAME
          FROM (
            SELECT SOL_ID,
                   CIFID,
                   FORACID,
                   ACCT_NAME,
                   FILE_NAME,
                   ROW_NUMBER() OVER (
                     PARTITION BY FILE_NAME
                     ORDER BY NVL(LCHG_TIME, RCRE_TIME) DESC
                   ) rn
            FROM HPSP_FYEAR_STMT_DET
            WHERE FILE_NAME IN (
              SELECT DISTINCT COLUMN_VALUE
              FROM TABLE(P_FILE_NAMES)
            )
          )
          WHERE rn = 1
        ) m
          ON m.FILE_NAME = f.FILE_NAME
    ) s
      ON (l.FILE_NAME = s.FILE_NAME)
    WHEN MATCHED THEN UPDATE SET
      l.STATUS = CASE
                   WHEN s.MISSING = 1 THEN 'META_NOT_FOUND'
                   ELSE 'READY'
                 END,
      l.DESCRIPTION = CASE
                        WHEN s.MISSING = 1 THEN 'metadata missing'
                        ELSE ''
                      END,
      l.SOL_ID = CASE
                   WHEN s.MISSING = 1 THEN l.SOL_ID
                   ELSE s.SOL_ID
                 END,
      l.CIFID = CASE
                  WHEN s.MISSING = 1 THEN l.CIFID
                  ELSE s.CIFID
                END,
      l.FORACID = CASE
                    WHEN s.MISSING = 1 THEN l.FORACID
                    ELSE s.FORACID
                  END,
      l.UPDATEDDATE = SYSTIMESTAMP;

    OPEN P_OUT FOR
      SELECT l.FILE_NAME,
             l.DOCID,
             CASE
               WHEN l.STATUS = 'META_NOT_FOUND' THEN 'META_NOT_FOUND'
               WHEN l.STATUS = 'FAILED_DB' THEN 'FAILED_DB'
               ELSE 'READY'
             END AS PRE_STATUS,
             l.STATUS AS LOG_STATUS,
             l.DESCRIPTION,
             m.SOL_ID,
             m.CIFID,
             m.FORACID,
             m.ACCT_NAME
        FROM FINACLE_STMT_LOG l
        LEFT JOIN (
          SELECT SOL_ID, CIFID, FORACID, ACCT_NAME, FILE_NAME
            FROM (
              SELECT SOL_ID,
                     CIFID,
                     FORACID,
                     ACCT_NAME,
                     FILE_NAME,
                     ROW_NUMBER() OVER (
                       PARTITION BY FILE_NAME
                       ORDER BY NVL(LCHG_TIME, RCRE_TIME) DESC
                     ) rn
                FROM HPSP_FYEAR_STMT_DET
               WHERE FILE_NAME IN (
                 SELECT DISTINCT COLUMN_VALUE
                 FROM TABLE(P_FILE_NAMES)
               )
            )
           WHERE rn = 1
        ) m
          ON m.FILE_NAME = l.FILE_NAME
       WHERE l.FILE_NAME IN (
         SELECT DISTINCT COLUMN_VALUE
         FROM TABLE(P_FILE_NAMES)
       );

  EXCEPTION
    WHEN OTHERS THEN
      v_err_msg := SUBSTR(SQLERRM, 1, 200);

      FORALL i IN 1 .. P_FILE_NAMES.COUNT
        UPDATE FINACLE_STMT_LOG
           SET STATUS = 'FAILED_DB',
               DESCRIPTION = v_err_msg,
               UPDATEDDATE = SYSTIMESTAMP
         WHERE FILE_NAME = P_FILE_NAMES(i);

      RAISE;
  END PREPARE_FILES;

  PROCEDURE FINALIZE_DB(
    P_RECS IN T_FINALIZE_TAB
  ) IS
  BEGIN
    FORALL i IN 1 .. P_RECS.COUNT
      UPDATE FINACLE_STMT_LOG
         SET STATUS = CASE
                        WHEN P_RECS(i).STATUS = 'UPLOAD_SUCCESS'
                        THEN 'DB_COMMITTED_PENDING_MOVE'
                        ELSE P_RECS(i).STATUS
                      END,
             DESCRIPTION = SUBSTR(NVL(P_RECS(i).DESCRIPTION, ''), 1, 200),
             FY_YEARS = NVL(P_RECS(i).FY_YEARS, FY_YEARS),
             FILESIZE = NVL(P_RECS(i).FILESIZE, FILESIZE),
             FILEPATH = NVL(P_RECS(i).SOURCE_PATH, FILEPATH),
             SOL_ID = NVL(P_RECS(i).SOL_ID, SOL_ID),
             CIFID = NVL(P_RECS(i).CIFID, CIFID),
             FORACID = NVL(P_RECS(i).FORACID, FORACID),
             UPDATEDDATE = SYSTIMESTAMP,
             DOCID = NVL(DOCID, P_RECS(i).DOCID)
       WHERE FILE_NAME = P_RECS(i).FILE_NAME;

    FOR i IN 1 .. P_RECS.COUNT LOOP
      IF P_RECS(i).STATUS = 'UPLOAD_SUCCESS' THEN
        MERGE INTO FINACLE_STMT t
        USING (SELECT P_RECS(i).DOCID AS DOCID FROM dual) s
           ON (t.DOCID = s.DOCID)
        WHEN MATCHED THEN UPDATE SET
          UPDATEDDATE = SYSTIMESTAMP,
          SOL_ID = P_RECS(i).SOL_ID,
          CIFID = P_RECS(i).CIFID,
          FORACID = P_RECS(i).FORACID,
          ACCT_NAME = P_RECS(i).ACCT_NAME,
          FILE_NAME = P_RECS(i).FILE_NAME,
          FY_YEARS = P_RECS(i).FY_YEARS,
          FILESIZE = P_RECS(i).FILESIZE,
          FILEPATH = P_RECS(i).SOURCE_PATH,
          FILE_EXTENSION = P_RECS(i).FILE_EXTENSION
        WHEN NOT MATCHED THEN INSERT (
          DOCID, CREATEDDATE, UPDATEDDATE, SOL_ID, CIFID, FORACID,
          ACCT_NAME, FILE_NAME, FY_YEARS, FILESIZE, FILEPATH, FILE_EXTENSION
        )
        VALUES (
          P_RECS(i).DOCID, SYSTIMESTAMP, SYSTIMESTAMP,
          P_RECS(i).SOL_ID, P_RECS(i).CIFID, P_RECS(i).FORACID,
          P_RECS(i).ACCT_NAME, P_RECS(i).FILE_NAME,
          P_RECS(i).FY_YEARS, P_RECS(i).FILESIZE,
          P_RECS(i).SOURCE_PATH, P_RECS(i).FILE_EXTENSION
        );
      END IF;
    END LOOP;
  END FINALIZE_DB;

  PROCEDURE FINALIZE_MOVE(
    P_RECS IN T_MOVE_TAB
  ) IS
  BEGIN
    FORALL i IN 1 .. P_RECS.COUNT
      UPDATE FINACLE_STMT_LOG
         SET STATUS = CASE
                        WHEN P_RECS(i).MOVE_STATUS = 'MOVED_SUCCESS' THEN 'SUCCESS'
                        WHEN P_RECS(i).MOVE_STATUS = 'MOVE_FAILED' THEN 'FAILED_MOVE'
                        ELSE STATUS
                      END,
             DESCRIPTION = SUBSTR(NVL(P_RECS(i).DESCRIPTION, ''), 1, 200),
             FILEPATH = NVL(P_RECS(i).FINAL_PATH, FILEPATH),
             FILESIZE = NVL(P_RECS(i).FILESIZE, FILESIZE),
             UPDATEDDATE = SYSTIMESTAMP
       WHERE FILE_NAME = P_RECS(i).FILE_NAME;
  END FINALIZE_MOVE;

END FINACLE_BATCH_PKG;
/
