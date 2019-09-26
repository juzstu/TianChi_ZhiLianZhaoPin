-- 构造jd_sub_type的点击率
drop table if exists qyxs_5_folds_jd_type; 
create table qyxs_5_folds_jd_type
as
select t1.*, t2.jd_sub_type from
 qyxs_5_folds_user t1
left join
(select user_id, jd_no, jd_sub_type from qyxs_final_all_df) t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;

-------------------------------------------- train ----------------------------------------------------
-- 第一折
drop table if exists qyxs_ctr_jd_type_folds1; 
create table qyxs_ctr_jd_type_folds1
as
select f2.user_id, f2.jd_no,
f1.jp_sat_click_cnt, f1.jp_sat_all_cnt, f1.jp_sat_click_ratio,
f1.jp_dev_click_cnt, f1.jp_dev_click_ratio
from
(select jd_sub_type, sum(satisfied) as jp_sat_click_cnt, count(1) as jp_sat_all_cnt,
avg(satisfied)as jp_sat_click_ratio, sum(delivered) as jp_dev_click_cnt,
avg(delivered) as jp_dev_click_ratio
from
qyxs_5_folds_jd_type where folds!=1 and jd_sub_type != '\\N' group by jd_sub_type) f1
right join
(select user_id, jd_no, jd_sub_type from qyxs_5_folds_jd_type where folds=1) f2
on f2.jd_sub_type=f1.jd_sub_type;


-- 第二折
drop table if exists qyxs_ctr_jd_type_folds2; 
create table qyxs_ctr_jd_type_folds2
as
select f2.user_id, f2.jd_no,
f1.jp_sat_click_cnt, f1.jp_sat_all_cnt, f1.jp_sat_click_ratio,
f1.jp_dev_click_cnt, f1.jp_dev_click_ratio
from
(select jd_sub_type, sum(satisfied) as jp_sat_click_cnt, count(1) as jp_sat_all_cnt,
avg(satisfied)as jp_sat_click_ratio, sum(delivered) as jp_dev_click_cnt,
avg(delivered) as jp_dev_click_ratio
from
qyxs_5_folds_jd_type where folds!=2 and jd_sub_type != '\\N' group by jd_sub_type) f1
right join
(select user_id, jd_no, jd_sub_type from qyxs_5_folds_jd_type where folds=2) f2
on f2.jd_sub_type=f1.jd_sub_type;

-- 第三折
drop table if exists qyxs_ctr_jd_type_folds3; 
create table qyxs_ctr_jd_type_folds3
as
select f2.user_id, f2.jd_no,
f1.jp_sat_click_cnt, f1.jp_sat_all_cnt, f1.jp_sat_click_ratio,
f1.jp_dev_click_cnt, f1.jp_dev_click_ratio
from
(select jd_sub_type, sum(satisfied) as jp_sat_click_cnt, count(1) as jp_sat_all_cnt,
avg(satisfied)as jp_sat_click_ratio, sum(delivered) as jp_dev_click_cnt,
avg(delivered) as jp_dev_click_ratio
from
qyxs_5_folds_jd_type where folds!=3 and jd_sub_type != '\\N' group by jd_sub_type) f1
right join
(select user_id, jd_no, jd_sub_type from qyxs_5_folds_jd_type where folds=3) f2
on f2.jd_sub_type=f1.jd_sub_type;


-- 第四折
drop table if exists qyxs_ctr_jd_type_folds4; 
create table qyxs_ctr_jd_type_folds4
as
select f2.user_id, f2.jd_no,
f1.jp_sat_click_cnt, f1.jp_sat_all_cnt, f1.jp_sat_click_ratio,
f1.jp_dev_click_cnt, f1.jp_dev_click_ratio
from
(select jd_sub_type, sum(satisfied) as jp_sat_click_cnt, count(1) as jp_sat_all_cnt,
avg(satisfied)as jp_sat_click_ratio, sum(delivered) as jp_dev_click_cnt,
avg(delivered) as jp_dev_click_ratio
from
qyxs_5_folds_jd_type where folds!=4 and jd_sub_type != '\\N' group by jd_sub_type) f1
right join
(select user_id, jd_no, jd_sub_type from qyxs_5_folds_jd_type where folds=4) f2
on f2.jd_sub_type=f1.jd_sub_type;

-- 第五折
drop table if exists qyxs_ctr_jd_type_folds5; 
create table qyxs_ctr_jd_type_folds5
as
select f2.user_id, f2.jd_no,
f1.jp_sat_click_cnt, f1.jp_sat_all_cnt, f1.jp_sat_click_ratio,
f1.jp_dev_click_cnt, f1.jp_dev_click_ratio
from
(select jd_sub_type, sum(satisfied) as jp_sat_click_cnt, count(1) as jp_sat_all_cnt,
avg(satisfied)as jp_sat_click_ratio, sum(delivered) as jp_dev_click_cnt,
avg(delivered) as jp_dev_click_ratio
from
qyxs_5_folds_jd_type where folds!=5 and jd_sub_type != '\\N' group by jd_sub_type) f1
right join
(select user_id, jd_no, jd_sub_type from qyxs_5_folds_jd_type where folds=5) f2
on f2.jd_sub_type=f1.jd_sub_type;


-- 合并五折数据并填充空值为-1
drop table if exists qyxs_ctr_jp_sub_train; 
create table qyxs_ctr_jp_sub_train
as
select t.user_id, t.jd_no, 
COALESCE(jp_sat_click_cnt, -1) as jp_sat_click_cnt,
COALESCE(jp_sat_all_cnt, -1) as jp_sat_all_cnt,
COALESCE(jp_sat_click_ratio, -1) as jp_sat_click_ratio,
COALESCE(jp_dev_click_cnt, -1) as jp_dev_click_cnt,
COALESCE(jp_dev_click_ratio, -1) as jp_dev_click_ratio
from
(
select * from qyxs_ctr_jd_type_folds1
UNION ALL 
select * from qyxs_ctr_jd_type_folds2
UNION ALL 
select * from qyxs_ctr_jd_type_folds3
UNION ALL 
select * from qyxs_ctr_jd_type_folds4
UNION ALL 
select * from qyxs_ctr_jd_type_folds5
) t;


-------------------------------------------- test ----------------------------------------------------
drop table if exists qyxs_ctr_jp_sub_test; 
create table qyxs_ctr_jp_sub_test
as
select f2.user_id, f2.jd_no,
COALESCE(jp_sat_click_cnt, -1) as jp_sat_click_cnt,
COALESCE(jp_sat_all_cnt, -1) as jp_sat_all_cnt,
COALESCE(jp_sat_click_ratio, -1) as jp_sat_click_ratio,
COALESCE(jp_dev_click_cnt, -1) as jp_dev_click_cnt,
COALESCE(jp_dev_click_ratio, -1) as jp_dev_click_ratio
from
(select jd_sub_type, sum(satisfied) as jp_sat_click_cnt, count(1) as jp_sat_all_cnt,
avg(satisfied)as jp_sat_click_ratio, sum(delivered) as jp_dev_click_cnt,
avg(delivered) as jp_dev_click_ratio
from
qyxs_5_folds_jd_type where jd_sub_type != '\\N' group by jd_sub_type) f1
right join
(select user_id, jd_no, jd_sub_type from qyxs_final_all_df where satisfied=-1) f2
on f2.jd_sub_type=f1.jd_sub_type;