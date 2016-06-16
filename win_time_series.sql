/* ----------------------------------------------------------------------- *//**

@file win_time_series.sql

@brief Windowing function utilities that go beyond existing window function capabilities in GPDB/HAWQ.

@author Written by Rashmi Raghu, Chris Rawles
@date 14 June 2016

 *//* ----------------------------------------------------------------------- */

 /**
  * @brief ABC
  * 1) Regular case: ID column is present
  + ID is contiguous
  + ID values increment by 1
  + val column is a number data type (int / bigint / float / float8 / numeric)
  *
  * @param XYZ
  *
  */

 CREATE OR REPLACE FUNCTION PDLTOOLS_SCHEMA.window_ts(data_tab TEXT, id TEXT, val TEXT, output_tab TEXT, win_size INTERVAL, win_slide_size INTERVAL)
 RETURNS VOID AS
 $$

     BEGIN
        PERFORM PDLTOOLS_SCHEMA.__wints_window_time_series(data_tab, id, val, output_tab, win_size, win_slide_size);
        RETURN;
     END;

 $$
 LANGUAGE PLPGSQL;


 /**
  * @brief ABC
  *
  * @param XYZ
  *
  */

--CREATE OR REPLACE FUNCTION PDLTOOLS_SCHEMA.__wints_window_time_series(data_tab TEXT, id TEXT, val TEXT, output_tab TEXT, win_size bigint, win_slide_size bigint)
CREATE OR REPLACE FUNCTION wintest.wints_window_time_series(data_tab TEXT, id TEXT, val TEXT, output_tab TEXT, win_size bigint, win_slide_size bigint)
RETURNS VOID AS
$$
    DECLARE
        sql TEXT;
        rid_first BIGINT;
        rid_last BIGINT;
        ct_rows_temp_tbl BIGINT;
    BEGIN
        sql := 'select min(rid) from ' ||data_tab|| ';';
        EXECUTE sql INTO rid_first;

        sql := 'select max(rid) from ' ||data_tab|| ';';
        EXECUTE sql INTO rid_last;

        EXECUTE 'drop table if exists temp_pdltools_win_comp_id_tbl;';
        sql := 'create temp table temp_pdltools_win_comp_id_tbl as
            select *, win_start_id/' ||win_slide_size|| ' as win_id, win_internal_comp_id + win_start_id - 1 as win_external_comp_id from
            (
              select generate_series(1,' ||win_size|| ',1) as win_internal_comp_id
            ) t1,
            (
              select generate_series(' ||rid_first|| ',' ||rid_last|| ',' ||win_slide_size|| ') as win_start_id
            ) t2
        distributed by (win_id,win_internal_comp_id);
        ';
        EXECUTE sql;

        -- Debug the temp table
        EXECUTE 'select count(*) from temp_pdltools_win_comp_id_tbl;' INTO ct_rows_temp_tbl;
        RAISE NOTICE 'Count of rows in the temp table: %', ct_rows_temp_tbl;

        sql := 'create table ' ||output_tab|| '(win_id BIGINT, arr_rid BIGINT[], arr_val FLOAT8[]) distributed by (win_id);';
        EXECUTE sql;

        sql := '
            insert into ' ||output_tab|| '
            select
                win_id,
                array_agg(rid order by win_external_comp_id) as arr_rid,
                array_agg(val order by win_external_comp_id) as arr_val
            from
            (
              select * from
                  temp_pdltools_win_comp_id_tbl t1,
                  (select ' ||id|| ',' ||val|| ' from ' ||data_tab|| ') t2
              where t1.win_external_comp_id = t2.' ||id|| '
            ) t3
            group by win_id;
        ';
        EXECUTE sql;

        EXECUTE 'drop table if exists temp_pdltools_win_comp_id_tbl;';

        RETURN;
     END;

 $$
 LANGUAGE PLPGSQL;

-- Example usage:
-- select wintest.wints_window_time_series('wintest.test_tbl_01','rid','val','wintest.test_tbl_01_winout',7,5);

--========================================================
-- Initial code dev before creating a function out of it
--========================================================
-- select (rid-1)/7, rid from wintest.test_tbl_01 order by rid;
--
-- drop table if exists wintest.win_comp_id_tbl;
-- create table wintest.win_comp_id_tbl as
-- select *, win_start_id/10 as win_id, win_internal_comp_id + win_start_id - 1 as win_external_comp_id from
-- (
--   select generate_series(1,7,1) as win_internal_comp_id
-- ) t1,
-- (
--   select generate_series(1,30,10) as win_start_id
-- ) t2
-- --order by win_start_id, win_internal_comp_id
-- distributed randomly;
--
-- select
--     win_id, array_agg(rid order by win_external_comp_id) as arr_rid,
--     array_agg(win_external_comp_id order by win_external_comp_id) as arr_win_external_comp_id,
--     array_agg(val order by win_external_comp_id) as arr_val
-- from
-- (
--   select * from
--       wintest.win_comp_id_tbl t1,
--       wintest.test_tbl_01 t2
--   where t1.win_external_comp_id = t2.rid
-- ) t3
-- group by win_id
-- order by win_id;
