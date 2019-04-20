CREATE OR REPLACE PACKAGE pp_csv
AS
  /* тип коллекции заголовков CSV для хранения длинных заголовков,
     вариант использования:
      pp_csv.query2sheet(
        'SELECT EMPCODE AS "#", FIO AS "#" FROM employee WHERE ROWNUM < 10', 
        csv, 
        ';', 
        pp_csv.captions_va('_0123456789_0123456789_0123456789_', '_0123456789_0123456789_0123456789_') );
  */
  TYPE captions_va IS VARRAY(100) OF VARCHAR2(32767);

  PROCEDURE query2sheet(
    stmt IN VARCHAR2, -- текст SQL-запроса
    sheet IN OUT CLOB, -- объект CSV-страницы
    delimeter IN VARCHAR2 DEFAULT ';', -- разделитель CSV
    column_captions IN captions_va DEFAULT NULL -- коллекция заголовков CSV
    );
  
  PROCEDURE query2sheet(
    ref_cursor IN OUT SYS_REFCURSOR, -- ссылка на курсор исходной выборки (открытый)
    sheet IN OUT CLOB, -- объект CSV-страницы
    delimeter VARCHAR2 DEFAULT ';', -- разделитель CSV
    column_captions IN captions_va DEFAULT NULL -- коллекция заголовков CSV
    );
END;
/
CREATE OR REPLACE PACKAGE BODY pp_csv
AS
  c_line_break CONSTANT VARCHAR2(2) := chr(13) || chr(10);

PROCEDURE get_from_cur(
  cur PLS_INTEGER, 
  sheet IN OUT CLOB, 
  delimeter VARCHAR2 DEFAULT ';', 
  column_captions IN captions_va DEFAULT NULL)
AS
  cap CLOB;
  bod CLOB;
  cols DBMS_SQL.DESC_TAB2;
  ncols NUMBER;
  col_val_chr VARCHAR2(32767);
  j NUMBER := 1;
BEGIN
  DBMS_LOB.CREATETEMPORARY(cap, TRUE, DBMS_LOB.SESSION);
  DBMS_LOB.CREATETEMPORARY(bod, TRUE, DBMS_LOB.SESSION);

  DBMS_SQL.DESCRIBE_COLUMNS2(cur, ncols, cols);

  FOR i IN 1 .. ncols
  LOOP
    DBMS_SQL.DEFINE_COLUMN(cur, i, col_val_chr, 32767);
  END LOOP;

  FOR i IN 1 .. ncols
  LOOP
    IF cols(i).col_name = '#' THEN
      DBMS_LOB.APPEND(cap, column_captions(j) || delimeter);
      j := j + 1;
    ELSE
      DBMS_LOB.APPEND(cap, cols(i).col_name || delimeter);
    END IF;
  END LOOP;

  WHILE DBMS_SQL.FETCH_ROWS(cur) > 0
  LOOP
    FOR i IN 1 .. ncols
    LOOP
      DBMS_SQL.COLUMN_VALUE(cur, i, col_val_chr);
      DBMS_LOB.APPEND(bod, col_val_chr || delimeter);
    END LOOP;
    DBMS_LOB.APPEND(bod, c_line_break);
  END LOOP;

  DBMS_LOB.APPEND(sheet, cap);
  DBMS_LOB.APPEND(sheet, c_line_break);
  DBMS_LOB.APPEND(sheet, bod);
  
  DBMS_LOB.FREETEMPORARY(cap);
  DBMS_LOB.FREETEMPORARY(bod);

EXCEPTION
  WHEN OTHERS THEN
    sheet := NULL;

END;

PROCEDURE query2sheet(
  ref_cursor IN OUT SYS_REFCURSOR, 
  sheet IN OUT CLOB, 
  delimeter IN VARCHAR2 DEFAULT ';', 
  column_captions IN captions_va DEFAULT NULL)
AS
  cur PLS_INTEGER := DBMS_SQL.TO_CURSOR_NUMBER(ref_cursor);
BEGIN
  get_from_cur(cur, sheet, delimeter, column_captions);
  DBMS_SQL.CLOSE_CURSOR(cur);
END;

PROCEDURE query2sheet(
  stmt IN VARCHAR2, 
  sheet IN OUT CLOB, 
  delimeter IN VARCHAR2 DEFAULT ';', 
  column_captions IN captions_va DEFAULT NULL)
AS
  cur PLS_INTEGER := DBMS_SQL.OPEN_CURSOR;
  ignore INTEGER;
BEGIN
  DBMS_SQL.PARSE(cur, stmt, DBMS_SQL.NATIVE);
  ignore := DBMS_SQL.EXECUTE(cur);
  get_from_cur(cur, sheet, delimeter, column_captions);
  DBMS_SQL.CLOSE_CURSOR(cur);
END;

END;
/
