/* ----------------------------------------------------------------------- *//**

@file win_time_series.sql

@brief Windowing function utilities that go beyond existing window function capabilities in GPDB/HAWQ.

@author Written by Rashmi Raghu, Chris Rawles
@date 14 June 2016

 *//* ----------------------------------------------------------------------- */

 /**
  * @brief
  * 1) Regular case: ID column is present
  + ID is contiguous
  + ID values increment by 1
  + val column is a number data type (int / bigint / float / float8 / numeric)
  * 2) TS column is present but ID may not be
  + Difference between any two consecutive TS values are all the same
  + val column is a number data type (int / bigint / float / float8 / numeric)
  + win_size and win_slide_size cover a whole number of rows of data
  *
  * @param data_tab Table that contains the data. The table
  * is expected to be in the form of (rid BIGINT, val FLOAT8)
  * @param id Name of the column specifying data IDs.
  * @param val Name of the column containing data values.
  * @param win_size Number of data points that span the window.
  * @param win_slide_size Difference between starting point of one window and the next.
  * @param output_tab Name of the output table. The table will have following
  * form: (win_id BIGINT, arr_rid BIGINT[], arr_val FLOAT8[]). win_id represents
  * the window ID sequentially numbered, arr_rid is the array of rid (data ID)
  # values in the window, and arr_val is the array of all the values for the given window.
  *
  */

--============================
--Notes for documentation
--============================
-- *) Any rows with NULLs in the timestamp column will be ignored
-- *) If ending rid or ts value occurs before the end of the dataset, the last window for which aggregate is provided may incorporate
-- rows past the end row stated in the input if the window size extends past that end row -- give example here
--============================

DROP FUNCTION IF EXISTS window_time_series(text,text,text,text,bigint,bigint);
CREATE OR REPLACE FUNCTION window_time_series(data_tab TEXT, id TEXT, val TEXT, output_tab TEXT, win_size bigint, win_slide_size bigint)
RETURNS VOID AS
$$
    DECLARE
        sql TEXT;
        rid_first BIGINT;
        rid_last BIGINT;
    BEGIN
        sql := 'select min(rid) from ' ||data_tab|| ';';
        EXECUTE sql INTO rid_first;

        sql := 'select max(rid) from ' ||data_tab|| ';';
        EXECUTE sql INTO rid_last;

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
                        select ' ||id|| ' as rid,' ||val|| ' as val from ' ||data_tab|| '
                    ) t2
                where t1.win_external_comp_id = t2.rid
            ) t3
            group by win_id;
        ';
        EXECUTE sql;

        RETURN;
     END;

 $$
 LANGUAGE PLPGSQL;

--=====================
-- Example usage
--=====================
-- Uncomment below and run in GPDB

-- drop table if exists test_tbl;
-- create table test_tbl (rid bigint, ts timestamp without time zone, val float8) distributed by (rid);
-- copy test_tbl from stdin with delimiter '|';
-- 1  |  2016-01-10 00:00:00  |  1.2
-- 2  |  2016-01-10 00:00:30  |  1.27845909572784
-- 3  |  2016-01-10 00:01:00  |  1.35643446504023
-- 4  |  2016-01-10 00:01:30  |  1.43344536385591
-- 5  |  2016-01-10 00:02:00  |  1.50901699437495
-- 6  |  2016-01-10 00:02:30  |  1.58268343236509
-- 7  |  2016-01-10 00:03:00  |  1.65399049973955
-- 8  |  2016-01-10 00:03:30  |  1.72249856471595
-- 9  |  2016-01-10 00:04:00  |  1.78778525229247
-- 10  |  2016-01-10 00:04:30  |  1.84944804833018
-- 11  |  2016-01-10 00:05:00  |  1.90710678118655
-- 12  |  2016-01-10 00:05:30  |  1.96040596560003
-- 13  |  2016-01-10 00:06:00  |  2.00901699437495
-- 14  |  2016-01-10 00:06:30  |  2.05264016435409
-- 15  |  2016-01-10 00:07:00  |  2.09100652418837
-- 16  |  2016-01-10 00:07:30  |  2.12387953251129
-- 17  |  2016-01-10 00:08:00  |  2.15105651629515
-- 18  |  2016-01-10 00:08:30  |  2.17236992039768
-- 19  |  2016-01-10 00:09:00  |  2.18768834059514
-- 20  |  2016-01-10 00:09:30  |  2.19691733373313
-- 21  |  2016-01-10 00:10:00  |  2.2
-- 22  |  2016-01-10 00:10:30  |  2.19691733373313
-- 23  |  2016-01-10 00:11:00  |  2.18768834059514
-- 24  |  2016-01-10 00:11:30  |  2.17236992039768
-- 25  |  2016-01-10 00:12:00  |  2.15105651629515
-- 26  |  2016-01-10 00:12:30  |  2.12387953251129
-- 27  |  2016-01-10 00:13:00  |  2.09100652418837
-- 28  |  2016-01-10 00:13:30  |  2.05264016435409
-- 29  |  2016-01-10 00:14:00  |  2.00901699437495
-- 30  |  2016-01-10 00:14:30  |  1.96040596560003
-- 31  |  2016-01-10 00:15:00  |  1.90710678118655
-- 32  |  2016-01-10 00:15:30  |  1.84944804833018
-- 33  |  2016-01-10 00:16:00  |  1.78778525229247
-- 34  |  2016-01-10 00:16:30  |  1.72249856471595
-- 35  |  2016-01-10 00:17:00  |  1.65399049973955
-- 36  |  2016-01-10 00:17:30  |  1.58268343236509
-- 37  |  2016-01-10 00:18:00  |  1.50901699437495
-- 38  |  2016-01-10 00:18:30  |  1.43344536385591
-- 39  |  2016-01-10 00:19:00  |  1.35643446504023
-- 40  |  2016-01-10 00:19:30  |  1.27845909572784
-- 41  |  2016-01-10 00:20:00  |  1.2
-- 42  |  2016-01-10 00:20:30  |  1.12154090427216
-- 43  |  2016-01-10 00:21:00  |  1.04356553495977
-- 44  |  2016-01-10 00:21:30  |  0.966554636144095
-- 45  |  2016-01-10 00:22:00  |  0.890983005625053
-- 46  |  2016-01-10 00:22:30  |  0.81731656763491
-- 47  |  2016-01-10 00:23:00  |  0.746009500260453
-- 48  |  2016-01-10 00:23:30  |  0.677501435284051
-- 49  |  2016-01-10 00:24:00  |  0.612214747707527
-- 50  |  2016-01-10 00:24:30  |  0.550551951669817
-- 51  |  2016-01-10 00:25:00  |  0.492893218813452
-- 52  |  2016-01-10 00:25:30  |  0.439594034399969
-- 53  |  2016-01-10 00:26:00  |  0.390983005625053
-- 54  |  2016-01-10 00:26:30  |  0.347359835645908
-- 55  |  2016-01-10 00:27:00  |  0.308993475811632
-- 56  |  2016-01-10 00:27:30  |  0.276120467488713
-- 57  |  2016-01-10 00:28:00  |  0.248943483704846
-- 58  |  2016-01-10 00:28:30  |  0.227630079602324
-- 59  |  2016-01-10 00:29:00  |  0.212311659404862
-- 60  |  2016-01-10 00:29:30  |  0.203082666266872
-- 61  |  2016-01-10 00:30:00  |  0.2
-- 62  |  2016-01-10 00:30:30  |  0.203082666266872
-- 63  |  2016-01-10 00:31:00  |  0.212311659404862
-- 64  |  2016-01-10 00:31:30  |  0.227630079602323
-- 65  |  2016-01-10 00:32:00  |  0.248943483704846
-- 66  |  2016-01-10 00:32:30  |  0.276120467488713
-- 67  |  2016-01-10 00:33:00  |  0.308993475811632
-- 68  |  2016-01-10 00:33:30  |  0.347359835645907
-- 69  |  2016-01-10 00:34:00  |  0.390983005625052
-- 70  |  2016-01-10 00:34:30  |  0.439594034399969
-- 71  |  2016-01-10 00:35:00  |  0.492893218813452
-- 72  |  2016-01-10 00:35:30  |  0.550551951669817
-- 73  |  2016-01-10 00:36:00  |  0.612214747707527
-- 74  |  2016-01-10 00:36:30  |  0.67750143528405
-- 75  |  2016-01-10 00:37:00  |  0.746009500260453
-- 76  |  2016-01-10 00:37:30  |  0.81731656763491
-- 77  |  2016-01-10 00:38:00  |  0.890983005625052
-- 78  |  2016-01-10 00:38:30  |  0.966554636144095
-- 79  |  2016-01-10 00:39:00  |  1.04356553495977
-- 80  |  2016-01-10 00:39:30  |  1.12154090427216
-- 81  |  2016-01-10 00:40:00  |  1.2
-- 82  |  2016-01-10 00:40:30  |  1.27845909572784
-- 83  |  2016-01-10 00:41:00  |  1.35643446504023
-- 84  |  2016-01-10 00:41:30  |  1.43344536385591
-- 85  |  2016-01-10 00:42:00  |  1.50901699437495
-- 86  |  2016-01-10 00:42:30  |  1.58268343236509
-- 87  |  2016-01-10 00:43:00  |  1.65399049973955
-- 88  |  2016-01-10 00:43:30  |  1.72249856471595
-- 89  |  2016-01-10 00:44:00  |  1.78778525229247
-- 90  |  2016-01-10 00:44:30  |  1.84944804833018
-- 91  |  2016-01-10 00:45:00  |  1.90710678118655
-- 92  |  2016-01-10 00:45:30  |  1.96040596560003
-- 93  |  2016-01-10 00:46:00  |  2.00901699437495
-- 94  |  2016-01-10 00:46:30  |  2.05264016435409
-- 95  |  2016-01-10 00:47:00  |  2.09100652418837
-- 96  |  2016-01-10 00:47:30  |  2.12387953251129
-- 97  |  2016-01-10 00:48:00  |  2.15105651629515
-- 98  |  2016-01-10 00:48:30  |  2.17236992039768
-- 99  |  2016-01-10 00:49:00  |  2.18768834059514
-- 100  |  2016-01-10 00:49:30  |  2.19691733373313
-- 101  |  2016-01-10 00:50:00  |  2.2
-- 102  |  2016-01-10 00:50:30  |  2.19691733373313
-- 103  |  2016-01-10 00:51:00  |  2.18768834059514
-- 104  |  2016-01-10 00:51:30  |  2.17236992039768
-- 105  |  2016-01-10 00:52:00  |  2.15105651629515
-- 106  |  2016-01-10 00:52:30  |  2.12387953251129
-- 107  |  2016-01-10 00:53:00  |  2.09100652418837
-- 108  |  2016-01-10 00:53:30  |  2.05264016435409
-- 109  |  2016-01-10 00:54:00  |  2.00901699437495
-- 110  |  2016-01-10 00:54:30  |  1.96040596560003
-- 111  |  2016-01-10 00:55:00  |  1.90710678118655
-- 112  |  2016-01-10 00:55:30  |  1.84944804833018
-- 113  |  2016-01-10 00:56:00  |  1.78778525229247
-- 114  |  2016-01-10 00:56:30  |  1.72249856471595
-- 115  |  2016-01-10 00:57:00  |  1.65399049973955
-- 116  |  2016-01-10 00:57:30  |  1.58268343236509
-- 117  |  2016-01-10 00:58:00  |  1.50901699437495
-- 118  |  2016-01-10 00:58:30  |  1.43344536385591
-- 119  |  2016-01-10 00:59:00  |  1.35643446504023
-- 120  |  2016-01-10 00:59:30  |  1.27845909572785
-- 121  |  2016-01-10 01:00:00  |  1.2
-- 122  |  2016-01-10 01:00:30  |  1.12154090427215
-- 123  |  2016-01-10 01:01:00  |  1.04356553495977
-- 124  |  2016-01-10 01:01:30  |  0.966554636144095
-- 125  |  2016-01-10 01:02:00  |  0.890983005625053
-- 126  |  2016-01-10 01:02:30  |  0.817316567634912
-- 127  |  2016-01-10 01:03:00  |  0.746009500260453
-- 128  |  2016-01-10 01:03:30  |  0.677501435284052
-- 129  |  2016-01-10 01:04:00  |  0.612214747707527
-- 130  |  2016-01-10 01:04:30  |  0.550551951669816
-- 131  |  2016-01-10 01:05:00  |  0.492893218813453
-- 132  |  2016-01-10 01:05:30  |  0.439594034399968
-- 133  |  2016-01-10 01:06:00  |  0.390983005625053
-- 134  |  2016-01-10 01:06:30  |  0.347359835645908
-- 135  |  2016-01-10 01:07:00  |  0.308993475811633
-- 136  |  2016-01-10 01:07:30  |  0.276120467488714
-- 137  |  2016-01-10 01:08:00  |  0.248943483704847
-- 138  |  2016-01-10 01:08:30  |  0.227630079602323
-- 139  |  2016-01-10 01:09:00  |  0.212311659404862
-- 140  |  2016-01-10 01:09:30  |  0.203082666266872
-- 141  |  2016-01-10 01:10:00  |  0.2
-- 142  |  2016-01-10 01:10:30  |  0.203082666266872
-- 143  |  2016-01-10 01:11:00  |  0.212311659404862
-- 144  |  2016-01-10 01:11:30  |  0.227630079602323
-- 145  |  2016-01-10 01:12:00  |  0.248943483704846
-- 146  |  2016-01-10 01:12:30  |  0.276120467488713
-- 147  |  2016-01-10 01:13:00  |  0.308993475811632
-- 148  |  2016-01-10 01:13:30  |  0.347359835645908
-- 149  |  2016-01-10 01:14:00  |  0.390983005625052
-- 150  |  2016-01-10 01:14:30  |  0.439594034399969
-- 151  |  2016-01-10 01:15:00  |  0.492893218813451
-- 152  |  2016-01-10 01:15:30  |  0.550551951669816
-- 153  |  2016-01-10 01:16:00  |  0.612214747707526
-- 154  |  2016-01-10 01:16:30  |  0.677501435284051
-- 155  |  2016-01-10 01:17:00  |  0.746009500260454
-- 156  |  2016-01-10 01:17:30  |  0.817316567634909
-- 157  |  2016-01-10 01:18:00  |  0.890983005625052
-- 158  |  2016-01-10 01:18:30  |  0.966554636144093
-- 159  |  2016-01-10 01:19:00  |  1.04356553495977
-- 160  |  2016-01-10 01:19:30  |  1.12154090427215
-- 161  |  2016-01-10 01:20:00  |  1.2
-- 162  |  2016-01-10 01:20:30  |  1.27845909572784
-- 163  |  2016-01-10 01:21:00  |  1.35643446504023
-- 164  |  2016-01-10 01:21:30  |  1.4334453638559
-- 165  |  2016-01-10 01:22:00  |  1.50901699437495
-- 166  |  2016-01-10 01:22:30  |  1.58268343236509
-- 167  |  2016-01-10 01:23:00  |  1.65399049973955
-- 168  |  2016-01-10 01:23:30  |  1.72249856471595
-- 169  |  2016-01-10 01:24:00  |  1.78778525229247
-- 170  |  2016-01-10 01:24:30  |  1.84944804833018
-- 171  |  2016-01-10 01:25:00  |  1.90710678118655
-- 172  |  2016-01-10 01:25:30  |  1.96040596560003
-- 173  |  2016-01-10 01:26:00  |  2.00901699437495
-- 174  |  2016-01-10 01:26:30  |  2.05264016435409
-- 175  |  2016-01-10 01:27:00  |  2.09100652418837
-- 176  |  2016-01-10 01:27:30  |  2.12387953251129
-- 177  |  2016-01-10 01:28:00  |  2.15105651629515
-- 178  |  2016-01-10 01:28:30  |  2.17236992039768
-- 179  |  2016-01-10 01:29:00  |  2.18768834059514
-- 180  |  2016-01-10 01:29:30  |  2.19691733373313
-- 181  |  2016-01-10 01:30:00  |  2.2
-- 182  |  2016-01-10 01:30:30  |  2.19691733373313
-- 183  |  2016-01-10 01:31:00  |  2.18768834059514
-- 184  |  2016-01-10 01:31:30  |  2.17236992039768
-- 185  |  2016-01-10 01:32:00  |  2.15105651629515
-- 186  |  2016-01-10 01:32:30  |  2.12387953251129
-- 187  |  2016-01-10 01:33:00  |  2.09100652418837
-- 188  |  2016-01-10 01:33:30  |  2.05264016435409
-- 189  |  2016-01-10 01:34:00  |  2.00901699437495
-- 190  |  2016-01-10 01:34:30  |  1.96040596560003
-- 191  |  2016-01-10 01:35:00  |  1.90710678118655
-- 192  |  2016-01-10 01:35:30  |  1.84944804833018
-- 193  |  2016-01-10 01:36:00  |  1.78778525229247
-- 194  |  2016-01-10 01:36:30  |  1.72249856471595
-- 195  |  2016-01-10 01:37:00  |  1.65399049973955
-- 196  |  2016-01-10 01:37:30  |  1.58268343236509
-- 197  |  2016-01-10 01:38:00  |  1.50901699437495
-- 198  |  2016-01-10 01:38:30  |  1.43344536385591
-- 199  |  2016-01-10 01:39:00  |  1.35643446504023
-- 200  |  2016-01-10 01:39:30  |  1.27845909572784
-- 201  |  2016-01-10 01:40:00  |  1.2
-- 202  |  2016-01-10 01:40:30  |  1.12154090427216
-- 203  |  2016-01-10 01:41:00  |  1.04356553495977
-- 204  |  2016-01-10 01:41:30  |  0.966554636144096
-- 205  |  2016-01-10 01:42:00  |  0.890983005625055
-- 206  |  2016-01-10 01:42:30  |  0.817316567634912
-- 207  |  2016-01-10 01:43:00  |  0.746009500260455
-- 208  |  2016-01-10 01:43:30  |  0.677501435284052
-- 209  |  2016-01-10 01:44:00  |  0.612214747707527
-- 210  |  2016-01-10 01:44:30  |  0.550551951669816
-- 211  |  2016-01-10 01:45:00  |  0.492893218813452
-- 212  |  2016-01-10 01:45:30  |  0.439594034399969
-- 213  |  2016-01-10 01:46:00  |  0.390983005625052
-- 214  |  2016-01-10 01:46:30  |  0.347359835645909
-- 215  |  2016-01-10 01:47:00  |  0.308993475811633
-- 216  |  2016-01-10 01:47:30  |  0.276120467488714
-- 217  |  2016-01-10 01:48:00  |  0.248943483704847
-- 218  |  2016-01-10 01:48:30  |  0.227630079602323
-- 219  |  2016-01-10 01:49:00  |  0.212311659404863
-- 220  |  2016-01-10 01:49:30  |  0.203082666266872
-- 221  |  2016-01-10 01:50:00  |  0.2
-- 222  |  2016-01-10 01:50:30  |  0.203082666266872
-- 223  |  2016-01-10 01:51:00  |  0.212311659404862
-- 224  |  2016-01-10 01:51:30  |  0.227630079602323
-- 225  |  2016-01-10 01:52:00  |  0.248943483704846
-- 226  |  2016-01-10 01:52:30  |  0.276120467488713
-- 227  |  2016-01-10 01:53:00  |  0.308993475811632
-- 228  |  2016-01-10 01:53:30  |  0.347359835645906
-- 229  |  2016-01-10 01:54:00  |  0.390983005625051
-- 230  |  2016-01-10 01:54:30  |  0.439594034399968
-- 231  |  2016-01-10 01:55:00  |  0.492893218813454
-- 232  |  2016-01-10 01:55:30  |  0.550551951669815
-- 233  |  2016-01-10 01:56:00  |  0.612214747707526
-- 234  |  2016-01-10 01:56:30  |  0.677501435284051
-- 235  |  2016-01-10 01:57:00  |  0.746009500260453
-- 236  |  2016-01-10 01:57:30  |  0.817316567634911
-- 237  |  2016-01-10 01:58:00  |  0.89098300562505
-- 238  |  2016-01-10 01:58:30  |  0.966554636144093
-- 239  |  2016-01-10 01:59:00  |  1.04356553495977
-- 240  |  2016-01-10 01:59:30  |  1.12154090427215
-- 241  |  2016-01-10 02:00:00  |  1.2
-- 242  |  2016-01-10 02:00:30  |  1.27845909572784
-- 243  |  2016-01-10 02:01:00  |  1.35643446504023
-- 244  |  2016-01-10 02:01:30  |  1.43344536385591
-- 245  |  2016-01-10 02:02:00  |  1.50901699437495
-- 246  |  2016-01-10 02:02:30  |  1.58268343236509
-- 247  |  2016-01-10 02:03:00  |  1.65399049973955
-- 248  |  2016-01-10 02:03:30  |  1.72249856471595
-- 249  |  2016-01-10 02:04:00  |  1.78778525229247
-- 250  |  2016-01-10 02:04:30  |  1.84944804833018
-- 251  |  2016-01-10 02:05:00  |  1.90710678118654
-- 252  |  2016-01-10 02:05:30  |  1.96040596560003
-- 253  |  2016-01-10 02:06:00  |  2.00901699437495
-- 254  |  2016-01-10 02:06:30  |  2.05264016435409
-- 255  |  2016-01-10 02:07:00  |  2.09100652418837
-- 256  |  2016-01-10 02:07:30  |  2.12387953251129
-- 257  |  2016-01-10 02:08:00  |  2.15105651629515
-- 258  |  2016-01-10 02:08:30  |  2.17236992039768
-- 259  |  2016-01-10 02:09:00  |  2.18768834059514
-- 260  |  2016-01-10 02:09:30  |  2.19691733373313
-- 261  |  2016-01-10 02:10:00  |  2.2
-- 262  |  2016-01-10 02:10:30  |  2.19691733373313
-- 263  |  2016-01-10 02:11:00  |  2.18768834059514
-- 264  |  2016-01-10 02:11:30  |  2.17236992039768
-- 265  |  2016-01-10 02:12:00  |  2.15105651629515
-- 266  |  2016-01-10 02:12:30  |  2.12387953251129
-- 267  |  2016-01-10 02:13:00  |  2.09100652418837
-- 268  |  2016-01-10 02:13:30  |  2.05264016435409
-- 269  |  2016-01-10 02:14:00  |  2.00901699437495
-- 270  |  2016-01-10 02:14:30  |  1.96040596560003
-- 271  |  2016-01-10 02:15:00  |  1.90710678118655
-- 272  |  2016-01-10 02:15:30  |  1.84944804833018
-- 273  |  2016-01-10 02:16:00  |  1.78778525229247
-- 274  |  2016-01-10 02:16:30  |  1.72249856471595
-- 275  |  2016-01-10 02:17:00  |  1.65399049973955
-- 276  |  2016-01-10 02:17:30  |  1.58268343236509
-- 277  |  2016-01-10 02:18:00  |  1.50901699437495
-- 278  |  2016-01-10 02:18:30  |  1.43344536385591
-- 279  |  2016-01-10 02:19:00  |  1.35643446504023
-- 280  |  2016-01-10 02:19:30  |  1.27845909572785
-- 281  |  2016-01-10 02:20:00  |  1.2
-- 282  |  2016-01-10 02:20:30  |  1.12154090427216
-- 283  |  2016-01-10 02:21:00  |  1.04356553495977
-- 284  |  2016-01-10 02:21:30  |  0.966554636144094
-- 285  |  2016-01-10 02:22:00  |  0.890983005625052
-- 286  |  2016-01-10 02:22:30  |  0.817316567634909
-- 287  |  2016-01-10 02:23:00  |  0.746009500260455
-- 288  |  2016-01-10 02:23:30  |  0.677501435284052
-- 289  |  2016-01-10 02:24:00  |  0.612214747707527
-- 290  |  2016-01-10 02:24:30  |  0.550551951669817
-- 291  |  2016-01-10 02:25:00  |  0.492893218813452
-- 292  |  2016-01-10 02:25:30  |  0.439594034399971
-- 293  |  2016-01-10 02:26:00  |  0.390983005625054
-- 294  |  2016-01-10 02:26:30  |  0.347359835645909
-- 295  |  2016-01-10 02:27:00  |  0.308993475811631
-- 296  |  2016-01-10 02:27:30  |  0.276120467488714
-- 297  |  2016-01-10 02:28:00  |  0.248943483704847
-- 298  |  2016-01-10 02:28:30  |  0.227630079602324
-- 299  |  2016-01-10 02:29:00  |  0.212311659404862
-- 300  |  2016-01-10 02:29:30  |  0.203082666266872
-- 301  |  2016-01-10 02:30:00  |  0.2
-- 302  |  2016-01-10 02:30:30  |  0.203082666266872
-- 303  |  2016-01-10 02:31:00  |  0.212311659404862
-- 304  |  2016-01-10 02:31:30  |  0.227630079602323
-- 305  |  2016-01-10 02:32:00  |  0.248943483704846
-- 306  |  2016-01-10 02:32:30  |  0.276120467488713
-- 307  |  2016-01-10 02:33:00  |  0.308993475811632
-- 308  |  2016-01-10 02:33:30  |  0.347359835645908
-- 309  |  2016-01-10 02:34:00  |  0.390983005625053
-- 310  |  2016-01-10 02:34:30  |  0.439594034399968
-- 311  |  2016-01-10 02:35:00  |  0.492893218813451
-- 312  |  2016-01-10 02:35:30  |  0.550551951669815
-- 313  |  2016-01-10 02:36:00  |  0.612214747707526
-- 314  |  2016-01-10 02:36:30  |  0.677501435284051
-- 315  |  2016-01-10 02:37:00  |  0.74600950026045
-- 316  |  2016-01-10 02:37:30  |  0.817316567634911
-- 317  |  2016-01-10 02:38:00  |  0.890983005625053
-- 318  |  2016-01-10 02:38:30  |  0.966554636144096
-- 319  |  2016-01-10 02:39:00  |  1.04356553495977
-- 320  |  2016-01-10 02:39:30  |  1.12154090427215
-- 321  |  2016-01-10 02:40:00  |  1.2
-- 322  |  2016-01-10 02:40:30  |  1.27845909572784
-- 323  |  2016-01-10 02:41:00  |  1.35643446504023
-- 324  |  2016-01-10 02:41:30  |  1.4334453638559
-- 325  |  2016-01-10 02:42:00  |  1.50901699437494
-- 326  |  2016-01-10 02:42:30  |  1.58268343236509
-- 327  |  2016-01-10 02:43:00  |  1.65399049973955
-- 328  |  2016-01-10 02:43:30  |  1.72249856471595
-- 329  |  2016-01-10 02:44:00  |  1.78778525229247
-- 330  |  2016-01-10 02:44:30  |  1.84944804833018
-- 331  |  2016-01-10 02:45:00  |  1.90710678118654
-- 332  |  2016-01-10 02:45:30  |  1.96040596560003
-- 333  |  2016-01-10 02:46:00  |  2.00901699437495
-- 334  |  2016-01-10 02:46:30  |  2.05264016435409
-- 335  |  2016-01-10 02:47:00  |  2.09100652418837
-- 336  |  2016-01-10 02:47:30  |  2.12387953251129
-- 337  |  2016-01-10 02:48:00  |  2.15105651629515
-- 338  |  2016-01-10 02:48:30  |  2.17236992039768
-- 339  |  2016-01-10 02:49:00  |  2.18768834059514
-- 340  |  2016-01-10 02:49:30  |  2.19691733373313
-- 341  |  2016-01-10 02:50:00  |  2.2
-- 342  |  2016-01-10 02:50:30  |  2.19691733373313
-- 343  |  2016-01-10 02:51:00  |  2.18768834059514
-- 344  |  2016-01-10 02:51:30  |  2.17236992039768
-- 345  |  2016-01-10 02:52:00  |  2.15105651629515
-- 346  |  2016-01-10 02:52:30  |  2.12387953251129
-- 347  |  2016-01-10 02:53:00  |  2.09100652418837
-- 348  |  2016-01-10 02:53:30  |  2.05264016435409
-- 349  |  2016-01-10 02:54:00  |  2.00901699437495
-- 350  |  2016-01-10 02:54:30  |  1.96040596560003
-- 351  |  2016-01-10 02:55:00  |  1.90710678118655
-- 352  |  2016-01-10 02:55:30  |  1.84944804833018
-- 353  |  2016-01-10 02:56:00  |  1.78778525229248
-- 354  |  2016-01-10 02:56:30  |  1.72249856471595
-- 355  |  2016-01-10 02:57:00  |  1.65399049973955
-- 356  |  2016-01-10 02:57:30  |  1.58268343236509
-- 357  |  2016-01-10 02:58:00  |  1.50901699437495
-- 358  |  2016-01-10 02:58:30  |  1.43344536385591
-- 359  |  2016-01-10 02:59:00  |  1.35643446504023
-- 360  |  2016-01-10 02:59:30  |  1.27845909572785
-- \.
--
-- drop table if exists test_tbl_winout_7_1;
-- select window_time_series('test_tbl','rid','val','test_tbl_winout_7_1',7,1);
--
-- drop table if exists test_tbl_winout_7_5;
-- select window_time_series('test_tbl','rid','val','test_tbl_winout_7_5',7,5);
--
-- drop table if exists test_tbl_winout_7_10;
-- select window_time_series('test_tbl','rid','val','test_tbl_winout_7_10',7,10);

DROP FUNCTION IF EXISTS wintest.window_time_series(text,text,text,text,interval,interval);
CREATE OR REPLACE FUNCTION wintest.window_time_series(data_tab TEXT, ts TEXT, val TEXT, output_tab TEXT, win_size interval, win_slide_size interval)
RETURNS VOID AS
$$
    DECLARE
        sql TEXT;
        rid_first BIGINT;
        rid_last BIGINT;
        datatype_ts TEXT;
        diff_ts INTERVAL;
        win_size_rows BIGINT;
        win_slide_size_rows BIGINT;
    BEGIN
        -- To find out: Does any user have access to read the information_schema.columns table or is it restricted to gpadmin users?
        sql := '
            select data_type from information_schema.columns
            where (table_schema || ' ||'''.'''|| ' || table_name) = ''' ||data_tab|| ''' and column_name = ''' ||ts|| ''';'
        ;
        EXECUTE sql INTO datatype_ts;
        RAISE NOTICE 'datatype_ts = %', datatype_ts;

        -- Assume the first timestamp in the dataset will have rid=1
        rid_first := 1;

        -- Assume that the last rid value will be the number of rows in the data table
        -- This will be the case if the timestamp column is monotonically increasing and
        -- the difference between every pair of consecutive timestamps is the same
        sql := 'select count(*) from ' ||data_tab|| ';';
        EXECUTE sql INTO rid_last;

        sql := '
            select diff_ts from (
                select ' ||ts|| ', lead(' ||ts|| ',1) over (order by ' ||ts|| ') - ' ||ts|| ' as diff_ts
                from (
                    select ' ||ts|| ' from ' ||data_tab|| ' order by ' ||ts|| ' limit 2
                ) t1
                order by 1 limit 1
            ) t2;'
        ;
        EXECUTE sql INTO diff_ts;

        -- Assumes that there are a whole number of rows of data in a window of win_size
        -- i.e that the division below will not result in a fraction amount
        win_size_rows := win_size / diff_ts;

        -- Assumes that there are a whole number of rows of data in a window of win_slide_size
        -- i.e that the division below will not result in a fraction amount
        win_slide_size_rows := win_slide_size / diff_ts;

        sql :=
            'create table ' ||output_tab|| '(win_id BIGINT, arr_rid BIGINT[], arr_ts ' ||datatype_ts||
                '[], arr_val FLOAT8[]) distributed by (win_id);';
        EXECUTE sql;

        sql := '
            insert into ' ||output_tab|| '
            select
                win_id,
                array_agg(rid order by win_external_comp_id) as arr_rid,
                array_agg(ts order by win_external_comp_id) as arr_ts,
                array_agg(val order by win_external_comp_id) as arr_val
            from
            (
                select * from
                    (
                        select *, win_start_id/' ||win_slide_size_rows|| ' as win_id, win_internal_comp_id + win_start_id - 1 as win_external_comp_id from
                        (
                          select generate_series(1,' ||win_size_rows|| ',1) as win_internal_comp_id
                        ) ta,
                        (
                          select generate_series(' ||rid_first|| ',' ||rid_last|| ',' ||win_slide_size_rows|| ') as win_start_id
                        ) tb
                    ) t1,
                    (
                        select row_number() over (order by ' ||ts|| ') as rid,' ||ts|| ' as ts,' ||val|| ' as val from ' ||data_tab|| '
                    ) t2
                where t1.win_external_comp_id = t2.rid
            ) t3
            group by win_id;
        ';
        EXECUTE sql;

        RETURN;
     END;

 $$
 LANGUAGE PLPGSQL;

 -- drop table if exists wintest.test_tbl_winout_1hr_30min;
 -- select wintest.window_time_series('wintest.test_tbl_01','ts','val','wintest.test_tbl_winout_1hr_30min','1 hour'::interval,'30 minutes'::interval);


DROP FUNCTION IF EXISTS wintest.window_time_series_gen(text,text,text,text,timestamp without time zone, timestamp without time zone,interval,interval);
CREATE OR REPLACE FUNCTION wintest.window_time_series_gen(
    data_tab TEXT,
    ts TEXT,
    val TEXT,
    output_tab TEXT,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    win_size interval,
    win_slide_size interval
)
RETURNS VOID AS
$$
    DECLARE
        sql TEXT;
        datatype_ts TEXT;

    BEGIN
         -- To find out: Does any user have access to read the information_schema.columns table or is it restricted to gpadmin users?
        sql := '
           select data_type from information_schema.columns
           where (table_schema || ' ||'''.'''|| ' || table_name) = ''' ||data_tab|| ''' and column_name = ''' ||ts|| ''';'
        ;
        EXECUTE sql INTO datatype_ts;
        RAISE NOTICE 'datatype_ts = %', datatype_ts;

        sql :=
            'create table ' ||output_tab|| '(win_id BIGINT, win_start_time ' ||datatype_ts|| ', win_end_time ' ||datatype_ts|| ',
            arr_rid BIGINT[], arr_ts ' ||datatype_ts|| '[], arr_val FLOAT8[]) distributed by (win_id);';
        EXECUTE sql;

        sql := '
            insert into ' ||output_tab|| '
            select
                win_id,
                bb as win_start_time,
                cc as win_end_time,
                array_agg(rid order by ts) as arr_rid,
                array_agg(ts order by ts) as arr_ts,
                array_agg(val order by ts) as arr_val
            from (
                select win_id, rid, ts, val, bb, cc from
                (
                    select row_number() over (order by ts) as rid, * from (
                        select ' ||ts|| ' as ts, ' ||val|| ' as val from ' ||data_tab|| '
                    ) ta1
                ) ta,
                (
                    select row_number() over (order by bb) - 1 as win_id, * from (
                        select
                            bb,
                            bb+''' ||win_size|| '''::interval as cc
                        from (
                            select generate_series(
                                ''' ||start_time|| '''::timestamp without time zone,
                                ''' ||end_time|| '''::timestamp without time zone,
                                ''' ||win_slide_size|| '''::interval
                            ) as bb
                        ) tb
                    ) tc
                    where cc is not null
                ) td
                where ts >= bb and ts < cc
            ) te
            group by win_id, bb, cc;
        ';
        EXECUTE sql;

        RETURN;
    END;
$$
LANGUAGE PLPGSQL;

-- Example code to test:
-- drop table if exists wintest.test_tbl_winout_1hr_30min;
-- select wintest.window_time_series_gen(
--     'wintest.test_tbl_01',
--     'ts',
--     'val',
--     'wintest.test_tbl_gen_winout_4p25min_2p25min',
--     '2016-01-10 00:00:00'::timestamp without time zone,
--     '2016-01-10 00:11:00'::timestamp without time zone,
--     '4.25 minutes'::interval,
--     '2.25 minutes'::interval
-- );
-- select * from wintest.test_tbl_gen_winout_4p25min_2p25min order by win_id;

--Add parition columns

--============================================
-- Prototyping space below
--============================================
-- Prototype / test for non-integer divisible window sizes and non-regular time series
select t1.aa as aa_t1, t2.aa as aa_t2, bb, cc from
    (select * from (select generate_series(0,10,1) as aa) ta, (select generate_series(425,1275,425)/100.0 as bb) tb where aa < bb) t1,
    (select * from (select generate_series(0,10,1) as aa) tc, (select generate_series(0,850,425)/100.0 as cc) td where aa >= cc) t2
where t1.aa = t2.aa
order by t1.aa, t2.aa;
--
select * from (
    select
        bb,
        lead(bb,1) over (order by bb) as cc
    from (
        select generate_series(0,1275,425)/100.0 as bb
    ) tb
) tc
where cc is not null
;
-- Without incorporating any sliding here
select * from
(
    select generate_series(0,10,1) as aa
) ta,
(
    select * from (
        select
            bb,
            lead(bb,1) over (order by bb) as cc
        from (
            select generate_series(0,1275,425)/100.0 as bb
        ) tb
    ) tc
    where cc is not null
) td
where aa >= bb and aa < cc
order by aa;
-- With incorporating sliding here
select * from
(
    select generate_series(0,10,1) as aa
) ta,
(
    select * from (
        select
            bb,
            bb+4.25 as cc
        from (
            select generate_series(0,1000,225)/100.0 as bb
        ) tb
    ) tc
    where cc is not null
) td
where aa >= bb and aa < cc
order by bb,cc,aa;
--Introduce window id now
select win_id, aa, bb, cc from
(
    select generate_series(0,10,1) as aa
) ta,
(
    select row_number() over (order by bb) - 1 as win_id, * from (
        select
            bb,
            bb+4.25 as cc
        from (
            select generate_series(0,1000,225)/100.0 as bb
        ) tb
    ) tc
    where cc is not null
) td
where aa >= bb and aa < cc
order by win_id,aa;
--The above works but now need to make it for timestamp column
select win_id, rid, aa, bb, cc from
(
    select row_number() over (order by aa) as rid, * from (
        select generate_series(
            '2016-01-10 00:00:00'::timestamp without time zone,
            '2016-01-10 02:59:30'::timestamp without time zone,
            '30 seconds'::interval
        ) as aa
    ) ta1
) ta,
(
    select row_number() over (order by bb) - 1 as win_id, * from (
        select
            bb,
            bb+'4.25 minutes'::interval as cc
        from (
            select generate_series(
                '2016-01-10 00:00:00'::timestamp without time zone,
                '2016-01-10 00:10:00'::timestamp without time zone,
                '2.25 minutes'::interval
            ) as bb
        ) tb
    ) tc
    where cc is not null
) td
where aa >= bb and aa < cc
order by win_id,aa;
--Try this on the test table now
select win_id, rid, ts, val, bb, cc from
(
    select row_number() over (order by ts) as rid, * from (
        select ts, val from wintest.test_tbl_01
    ) ta1
) ta,
(
    select row_number() over (order by bb) - 1 as win_id, * from (
        select
            bb,
            bb+'4.25 minutes'::interval as cc
        from (
            select generate_series(
                '2016-01-10 00:00:00'::timestamp without time zone,
                '2016-01-10 00:10:00'::timestamp without time zone,
                '2.25 minutes'::interval
            ) as bb
        ) tb
    ) tc
    where cc is not null
) td
where ts >= bb and ts < cc
order by win_id,ts;
--See if this form of code will work with row numbers too and not just time stamps
--If so the original code that worked for row numbers can be replaced with the above more general version
select win_id, rid, ts, val, bb, cc from
(
    select rid, ts, val from wintest.test_tbl_01
) ta,
(
    select row_number() over (order by bb) - 1 as win_id, * from (
        select
            bb,
            bb+7::bigint as cc
        from (
            select generate_series(
                1,
                25,
                3
            ) as bb
        ) tb
    ) tc
    where cc is not null
) td
where rid >= bb and rid < cc
order by win_id,rid;
--Yes, above seems to work - should change code to incorporate this
--Check code with partition columns in input
select pid, win_id, rid, ts, val, bb, cc from
(
    select row_number() over (partition by pid order by ts) as rid, * from (
        select row_number() over (order by ts)/10 as pid, ts, val from wintest.test_tbl_01
    ) ta1
) ta,
(
    select row_number() over (order by bb) - 1 as win_id, * from (
        select
            bb,
            bb+'4.25 minutes'::interval as cc
        from (
            select generate_series(
                '2016-01-10 00:00:00'::timestamp without time zone,
                '2016-01-10 00:10:00'::timestamp without time zone,
                '2.25 minutes'::interval
            ) as bb
        ) tb
    ) tc
    where cc is not null
) td
where ts >= bb and ts < cc
order by pid,win_id,ts;
