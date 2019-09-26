-- 根据user拆分5折，并构造五折的ctr的点击率特征
drop table if exists qyxs_5_folds_user; 
create table qyxs_5_folds_user
as
select t4.user_id, t4.jd_no, t4.satisfied, t4.delivered, t3.folds
from
(select t2.user_id, 
ntile(5) over (partition by t2.five_num order by t2.num) as folds
from
(select
t1.user_id,
t1.five_num, 
ROW_NUMBER() OVER(partition by t1.five_num order BY  t1.five_num desc) AS num
from
(select 
user_id, 
case 
    when count(1)>300 then 300
    else cast(round(count(1)/10) * 10 as int) end as five_num
from juz_uj_train_distinct group by user_id) t1) t2) t3 right join juz_uj_train_distinct t4
on t3.user_id=t4.user_id;

-------------------------------------------- train ----------------------------------------------------
-- 第一折
drop table if exists qyxs_ctr_folds1; 
create table qyxs_ctr_folds1
as
select f2.user_id, f2.jd_no, f1.sat_click_cnt, f1.sat_all_cnt, f1.sat_click_ratio,
f1.dev_click_cnt, f1.dev_click_ratio
from
(select jd_no, sum(satisfied) as sat_click_cnt, count(1) as sat_all_cnt,
avg(satisfied)as sat_click_ratio, sum(delivered) as dev_click_cnt,
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_user where folds!=1 group by jd_no) f1
right join
(select user_id, jd_no from qyxs_5_folds_user where folds=1) f2
on f2.jd_no=f1.jd_no;

-- 第二折
drop table if exists qyxs_ctr_folds2; 
create table qyxs_ctr_folds2
as
select f2.user_id, f2.jd_no, f1.sat_click_cnt, f1.sat_all_cnt, f1.sat_click_ratio,
f1.dev_click_cnt, f1.dev_click_ratio
from
(select jd_no, sum(satisfied) as sat_click_cnt, count(1) as sat_all_cnt,
avg(satisfied)as sat_click_ratio, sum(delivered) as dev_click_cnt,
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_user where folds!=2 group by jd_no) f1
right join
(select user_id, jd_no from qyxs_5_folds_user where folds=2) f2
on f2.jd_no=f1.jd_no;

-- 第三折
drop table if exists qyxs_ctr_folds3; 
create table qyxs_ctr_folds3
as
select f2.user_id, f2.jd_no, f1.sat_click_cnt, f1.sat_all_cnt, f1.sat_click_ratio,
f1.dev_click_cnt, f1.dev_click_ratio
from
(select jd_no, sum(satisfied) as sat_click_cnt, count(1) as sat_all_cnt,
avg(satisfied)as sat_click_ratio, sum(delivered) as dev_click_cnt,
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_user where folds!=3 group by jd_no) f1
right join
(select user_id, jd_no from qyxs_5_folds_user where folds=3) f2
on f2.jd_no=f1.jd_no;


-- 第四折
drop table if exists qyxs_ctr_folds4; 
create table qyxs_ctr_folds4
as
select f2.user_id, f2.jd_no, f1.sat_click_cnt, f1.sat_all_cnt, f1.sat_click_ratio,
f1.dev_click_cnt, f1.dev_click_ratio
from
(select jd_no, sum(satisfied) as sat_click_cnt, count(1) as sat_all_cnt,
avg(satisfied)as sat_click_ratio, sum(delivered) as dev_click_cnt,
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_user where folds!=4 group by jd_no) f1
right join
(select user_id, jd_no from qyxs_5_folds_user where folds=4) f2
on f2.jd_no=f1.jd_no;

-- 第五折
drop table if exists qyxs_ctr_folds5; 
create table qyxs_ctr_folds5
as
select f2.user_id, f2.jd_no, f1.sat_click_cnt, f1.sat_all_cnt, f1.sat_click_ratio,
f1.dev_click_cnt, f1.dev_click_ratio
from
(select jd_no, sum(satisfied) as sat_click_cnt, count(1) as sat_all_cnt,
avg(satisfied)as sat_click_ratio, sum(delivered) as dev_click_cnt,
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_user where folds!=5 group by jd_no) f1
right join
(select user_id, jd_no from qyxs_5_folds_user where folds=5) f2
on f2.jd_no=f1.jd_no;


-- 合并五折数据并填充空值为-1
drop table if exists qyxs_ctr_feats_sub_train; 
create table qyxs_ctr_feats_sub_train
as
select t.user_id, t.jd_no, 
COALESCE(sat_click_cnt, -1) as sat_click_cnt,
COALESCE(sat_all_cnt, -1) as sat_all_cnt,
COALESCE(sat_click_ratio, -1) as sat_click_ratio,
COALESCE(dev_click_cnt, -1) as dev_click_cnt,
COALESCE(dev_click_ratio, -1) as dev_click_ratio
from
(
select * from qyxs_ctr_folds1
UNION ALL 
select * from qyxs_ctr_folds2
UNION ALL 
select * from qyxs_ctr_folds3
UNION ALL 
select * from qyxs_ctr_folds4
UNION ALL 
select * from qyxs_ctr_folds5
) t;


-- 直接统计给测试集
drop table if exists qyxs_ctr_feats_sub_no_avg_test; 
create table qyxs_ctr_feats_sub_no_avg_test
as
select f2.user_id, f2.jd_no,
COALESCE(f1.sat_click_cnt, -1) as sat_click_cnt,
COALESCE(f1.sat_all_cnt, -1) as sat_all_cnt,
COALESCE(f1.sat_click_ratio, -1) as sat_click_ratio,
COALESCE(f1.dev_click_cnt, -1) as dev_click_cnt,
COALESCE(f1.dev_click_ratio, -1) as dev_click_ratio
from
(select jd_no, sum(satisfied) as sat_click_cnt, count(1) as sat_all_cnt,
avg(satisfied)as sat_click_ratio, sum(delivered) as dev_click_cnt,
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_user group by jd_no) f1
right join
juz_uj_test_distinct f2
on f2.jd_no=f1.jd_no;