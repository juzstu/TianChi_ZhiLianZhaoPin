-- 构造jd_title的点击率
drop table if exists qyxs_5_folds_jd_title; 
create table qyxs_5_folds_jd_title
as
select t1.*, t2.jd_title from
 qyxs_5_folds_user t1
left join
(select user_id, jd_no, jd_title from qyxs_final_all_df) t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;

-------------------------------------------- train ----------------------------------------------------
-- 第一折
drop table if exists qyxs_ctr_jd_title_folds1; 
create table qyxs_ctr_jd_title_folds1
as
select f2.user_id, f2.jd_no, f2.jd_title, f1.jt_sat_click_cnt, f1.jt_sat_all_cnt, f1.jt_sat_click_ratio,
f1.jt_dev_click_cnt, f1.jt_dev_click_ratio
from
(select jd_title, sum(satisfied) as jt_sat_click_cnt, count(1) as jt_sat_all_cnt,
avg(satisfied)as jt_sat_click_ratio, sum(delivered) as jt_dev_click_cnt,
avg(delivered) as jt_dev_click_ratio
from
qyxs_5_folds_jd_title where folds!=1 group by jd_title) f1
right join
(select user_id, jd_no, jd_title from qyxs_5_folds_jd_title where folds=1) f2
on f2.jd_title=f1.jd_title;


-- 第二折
drop table if exists qyxs_ctr_jd_title_folds2; 
create table qyxs_ctr_jd_title_folds2
as
select f2.user_id, f2.jd_no, f2.jd_title, f1.jt_sat_click_cnt, f1.jt_sat_all_cnt, f1.jt_sat_click_ratio,
f1.jt_dev_click_cnt, f1.jt_dev_click_ratio
from
(select jd_title, sum(satisfied) as jt_sat_click_cnt, count(1) as jt_sat_all_cnt,
avg(satisfied)as jt_sat_click_ratio, sum(delivered) as jt_dev_click_cnt,
avg(delivered) as jt_dev_click_ratio
from
qyxs_5_folds_jd_title where folds!=2 group by jd_title) f1
right join
(select user_id, jd_no, jd_title from qyxs_5_folds_jd_title where folds=2) f2
on f2.jd_title=f1.jd_title;

-- 第三折
drop table if exists qyxs_ctr_jd_title_folds3; 
create table qyxs_ctr_jd_title_folds3
as
select f2.user_id, f2.jd_no, f2.jd_title, f1.jt_sat_click_cnt, f1.jt_sat_all_cnt, f1.jt_sat_click_ratio,
f1.jt_dev_click_cnt, f1.jt_dev_click_ratio
from
(select jd_title, sum(satisfied) as jt_sat_click_cnt, count(1) as jt_sat_all_cnt,
avg(satisfied)as jt_sat_click_ratio, sum(delivered) as jt_dev_click_cnt,
avg(delivered) as jt_dev_click_ratio
from
qyxs_5_folds_jd_title where folds!=3 group by jd_title) f1
right join
(select user_id, jd_no, jd_title from qyxs_5_folds_jd_title where folds=3) f2
on f2.jd_title=f1.jd_title;


-- 第四折
drop table if exists qyxs_ctr_jd_title_folds4; 
create table qyxs_ctr_jd_title_folds4
as
select f2.user_id, f2.jd_no, f2.jd_title, f1.jt_sat_click_cnt, f1.jt_sat_all_cnt, f1.jt_sat_click_ratio,
f1.jt_dev_click_cnt, f1.jt_dev_click_ratio
from
(select jd_title, sum(satisfied) as jt_sat_click_cnt, count(1) as jt_sat_all_cnt,
avg(satisfied)as jt_sat_click_ratio, sum(delivered) as jt_dev_click_cnt,
avg(delivered) as jt_dev_click_ratio
from
qyxs_5_folds_jd_title where folds!=4 group by jd_title) f1
right join
(select user_id, jd_no, jd_title from qyxs_5_folds_jd_title where folds=4) f2
on f2.jd_title=f1.jd_title;

-- 第五折
drop table if exists qyxs_ctr_jd_title_folds5; 
create table qyxs_ctr_jd_title_folds5
as
select f2.user_id, f2.jd_no, f2.jd_title, f1.jt_sat_click_cnt, f1.jt_sat_all_cnt, f1.jt_sat_click_ratio,
f1.jt_dev_click_cnt, f1.jt_dev_click_ratio
from
(select jd_title, sum(satisfied) as jt_sat_click_cnt, count(1) as jt_sat_all_cnt,
avg(satisfied)as jt_sat_click_ratio, sum(delivered) as jt_dev_click_cnt,
avg(delivered) as jt_dev_click_ratio
from
qyxs_5_folds_jd_title where folds!=5 group by jd_title) f1
right join
(select user_id, jd_no, jd_title from qyxs_5_folds_jd_title where folds=5) f2
on f2.jd_title=f1.jd_title;


-- 合并五折数据并填充空值为-1
drop table if exists qyxs_ctr_jt_sub_train; 
create table qyxs_ctr_jt_sub_train
as
select t.user_id, t.jd_no, 
COALESCE(jt_sat_click_cnt, -1) as jt_sat_click_cnt,
COALESCE(jt_sat_all_cnt, -1) as jt_sat_all_cnt,
COALESCE(jt_sat_click_ratio, -1) as jt_sat_click_ratio,
COALESCE(jt_dev_click_cnt, -1) as jt_dev_click_cnt,
COALESCE(jt_dev_click_ratio, -1) as jt_dev_click_ratio
from
(
select * from qyxs_ctr_jd_title_folds1
UNION ALL 
select * from qyxs_ctr_jd_title_folds2
UNION ALL 
select * from qyxs_ctr_jd_title_folds3
UNION ALL 
select * from qyxs_ctr_jd_title_folds4
UNION ALL 
select * from qyxs_ctr_jd_title_folds5
) t;


-------------------------------------------- test ----------------------------------------------------
drop table if exists qyxs_ctr_jt_sub_test; 
create table qyxs_ctr_jt_sub_test
as
select f2.user_id, f2.jd_no,
COALESCE(jt_sat_click_cnt, -1) as jt_sat_click_cnt,
COALESCE(jt_sat_all_cnt, -1) as jt_sat_all_cnt,
COALESCE(jt_sat_click_ratio, -1) as jt_sat_click_ratio,
COALESCE(jt_dev_click_cnt, -1) as jt_dev_click_cnt,
COALESCE(jt_dev_click_ratio, -1) as jt_dev_click_ratio
from
(select jd_title, sum(satisfied) as jt_sat_click_cnt, count(1) as jt_sat_all_cnt,
avg(satisfied)as jt_sat_click_ratio, sum(delivered) as jt_dev_click_cnt,
avg(delivered) as jt_dev_click_ratio
from
qyxs_5_folds_jd_title group by jd_title) f1
right join
(select user_id, jd_no, jd_title from qyxs_final_all_df where satisfied=-1) f2
on f2.jd_title=f1.jd_title;