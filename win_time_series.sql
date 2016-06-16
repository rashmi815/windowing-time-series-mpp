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

DROP FUNCTION IF EXISTS wintest.window_time_series(text,text,text,text,bigint,bigint);
CREATE OR REPLACE FUNCTION wintest.window_time_series(data_tab TEXT, id TEXT, val TEXT, output_tab TEXT, win_size bigint, win_slide_size bigint)
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

        -- EXECUTE 'drop table if exists temp_pdltools_win_comp_id_tbl;';
        -- sql := 'create temp table temp_pdltools_win_comp_id_tbl as
        --     select *, win_start_id/' ||win_slide_size|| ' as win_id, win_internal_comp_id + win_start_id - 1 as win_external_comp_id from
        --     (
        --       select generate_series(1,' ||win_size|| ',1) as win_internal_comp_id
        --     ) t1,
        --     (
        --       select generate_series(' ||rid_first|| ',' ||rid_last|| ',' ||win_slide_size|| ') as win_start_id
        --     ) t2
        -- distributed by (win_id,win_internal_comp_id);
        -- ';
        -- EXECUTE sql;
        --
        -- -- Debug the temp table
        -- EXECUTE 'select count(*) from temp_pdltools_win_comp_id_tbl;' INTO ct_rows_temp_tbl;
        -- RAISE NOTICE 'Count of rows in the temp table: %', ct_rows_temp_tbl;

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
                    (
                        select *, win_start_id/' ||win_slide_size|| ' as win_id, win_internal_comp_id + win_start_id - 1 as win_external_comp_id from
                        (
                          select generate_series(1,' ||win_size|| ',1) as win_internal_comp_id
                        ) ta,
                        (
                          select generate_series(' ||rid_first|| ',' ||rid_last|| ',' ||win_slide_size|| ') as win_start_id
                        ) tb
                    ) t1,
                    (
                        select ' ||id|| ',' ||val|| ' from ' ||data_tab|| '
                    ) t2
                where t1.win_external_comp_id = t2.' ||id|| '
            ) t3
            group by win_id;
        ';
        EXECUTE sql;

        -- EXECUTE 'drop table if exists temp_pdltools_win_comp_id_tbl;';

        RETURN;
     END;

 $$
 LANGUAGE PLPGSQL;

-- Example usage:
-- select wintest.window_time_series('wintest.test_tbl_01','rid','val','wintest.test_tbl_01_winout',7,5);
