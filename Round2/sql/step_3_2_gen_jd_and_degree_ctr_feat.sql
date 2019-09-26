-- 构造jd和文凭的交叉点击率
drop table if exists qyxs_5_folds_jd_degree; 
create table qyxs_5_folds_jd_degree
as
select t1.*, t2.degree_map from
 qyxs_5_folds_user t1
left join
(select user_id, jd_no, degree_map
 from qyxs_final_all_df) t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;

-------------------------------------------- train ----------------------------------------------------
-- 第一折
drop table if exists qyxs_ctr_jd_degree_folds1; 
create table qyxs_ctr_jd_degree_folds1
as
select f2.user_id, f2.jd_no, 
f1.jdd_sat_click_ratio,
f1.jdd_dev_click_ratio
from
(select jd_no, degree_map,sum(satisfied) as jdd_sat_click_cnt, count(1) as jdd_sat_all_cnt,
avg(satisfied)as jdd_sat_click_ratio, sum(delivered) as jdd_dev_click_cnt,
avg(delivered) as jdd_dev_click_ratio
from
qyxs_5_folds_jd_degree where folds!=1 group by jd_no, degree_map) f1
right join
(select user_id, jd_no, degree_map from qyxs_5_folds_jd_degree where folds=1) f2
on f2.jd_no=f1.jd_no and f2.degree_map=f1.degree_map;


-- 第二折
drop table if exists qyxs_ctr_jd_degree_folds2; 
create table qyxs_ctr_jd_degree_folds2
as
select f2.user_id, f2.jd_no, 
f1.jdd_sat_click_ratio,
f1.jdd_dev_click_ratio
from
(select jd_no, degree_map,sum(satisfied) as jdd_sat_click_cnt, count(1) as jdd_sat_all_cnt,
avg(satisfied)as jdd_sat_click_ratio, sum(delivered) as jdd_dev_click_cnt,
avg(delivered) as jdd_dev_click_ratio
from
qyxs_5_folds_jd_degree where folds!=2 group by jd_no, degree_map) f1
right join
(select user_id, jd_no, degree_map from qyxs_5_folds_jd_degree where folds=2) f2
on f2.jd_no=f1.jd_no and f2.degree_map=f1.degree_map;

-- 第三折
drop table if exists qyxs_ctr_jd_degree_folds3; 
create table qyxs_ctr_jd_degree_folds3
as
select f2.user_id, f2.jd_no, 
f1.jdd_sat_click_ratio,
f1.jdd_dev_click_ratio
from
(select jd_no, degree_map,sum(satisfied) as jdd_sat_click_cnt, count(1) as jdd_sat_all_cnt,
avg(satisfied)as jdd_sat_click_ratio, sum(delivered) as jdd_dev_click_cnt,
avg(delivered) as jdd_dev_click_ratio
from
qyxs_5_folds_jd_degree where folds!=3 group by jd_no, degree_map) f1
right join
(select user_id, jd_no, degree_map from qyxs_5_folds_jd_degree where folds=3) f2
on f2.jd_no=f1.jd_no and f2.degree_map=f1.degree_map;


-- 第四折
drop table if exists qyxs_ctr_jd_degree_folds4; 
create table qyxs_ctr_jd_degree_folds4
as
select f2.user_id, f2.jd_no, 
f1.jdd_sat_click_ratio,
f1.jdd_dev_click_ratio
from
(select jd_no, degree_map,sum(satisfied) as jdd_sat_click_cnt, count(1) as jdd_sat_all_cnt,
avg(satisfied)as jdd_sat_click_ratio, sum(delivered) as jdd_dev_click_cnt,
avg(delivered) as jdd_dev_click_ratio
from
qyxs_5_folds_jd_degree where folds!=4 group by jd_no, degree_map) f1
right join
(select user_id, jd_no, degree_map from qyxs_5_folds_jd_degree where folds=4) f2
on f2.jd_no=f1.jd_no and f2.degree_map=f1.degree_map;

-- 第五折
drop table if exists qyxs_ctr_jd_degree_folds5; 
create table qyxs_ctr_jd_degree_folds5
as
select f2.user_id, f2.jd_no, 
f1.jdd_sat_click_ratio,
f1.jdd_dev_click_ratio
from
(select jd_no, degree_map,sum(satisfied) as jdd_sat_click_cnt, count(1) as jdd_sat_all_cnt,
avg(satisfied)as jdd_sat_click_ratio, sum(delivered) as jdd_dev_click_cnt,
avg(delivered) as jdd_dev_click_ratio
from
qyxs_5_folds_jd_degree where folds!=5 group by jd_no, degree_map) f1
right join
(select user_id, jd_no, degree_map from qyxs_5_folds_jd_degree where folds=5) f2
on f2.jd_no=f1.jd_no and f2.degree_map=f1.degree_map;


-- 合并五折数据并填充空值为-1
drop table if exists qyxs_ctr_jdd_sub_train; 
create table qyxs_ctr_jdd_sub_train
as
select t.user_id, t.jd_no, 
COALESCE(jdd_sat_click_ratio, -1) as jdd_sat_click_ratio,
COALESCE(jdd_dev_click_ratio, -1) as jdd_dev_click_ratio
from
(
select * from qyxs_ctr_jd_degree_folds1
UNION ALL 
select * from qyxs_ctr_jd_degree_folds2
UNION ALL 
select * from qyxs_ctr_jd_degree_folds3
UNION ALL 
select * from qyxs_ctr_jd_degree_folds4
UNION ALL 
select * from qyxs_ctr_jd_degree_folds5
) t;

-------------------------------------------- test ----------------------------------------------------
drop table if exists qyxs_ctr_jdd_sub_test; 
create table qyxs_ctr_jdd_sub_test
as
select f2.user_id, f2.jd_no,
COALESCE(f1.sat_click_ratio, -1) as jdd_sat_click_ratio,
COALESCE(f1.dev_click_ratio, -1) as jdd_dev_click_ratio
from
(select jd_no, degree_map, 
avg(satisfied)as sat_click_ratio, 
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_jd_degree group by jd_no, degree_map) f1
right join
(select user_id, jd_no, degree_map from qyxs_final_all_df where satisfied=-1) f2
on f2.jd_no=f1.jd_no and f2.degree_map=f1.degree_map;