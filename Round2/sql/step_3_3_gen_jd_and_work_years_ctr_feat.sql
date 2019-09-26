-- 构造jd和工作年限的交叉点击率
-- jd_nunique 分箱
drop table if exists qyxs_tmp_bin_work_years; 
create table qyxs_tmp_bin_work_years
as
select *, 
case 
    when work_years=0 then 0
    when work_years=1 then 1
    when work_years<=3 then 3
    when work_years<=5 then 5
    when work_years<=7 then 7
    when work_years<=10 then 10
    when work_years>10 then 11
    else -1 end as bin_wy from
qyxs_final_all_df;

-- 根据user拆分5折
drop table if exists qyxs_5_folds_jd_wy; 
create table qyxs_5_folds_jd_wy
as
select t1.*, t2.bin_wy from
 qyxs_5_folds_user t1
left join
(select user_id, jd_no, bin_wy
 from qyxs_tmp_bin_work_years) t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;

-------------------------------------------- train ----------------------------------------------------
-- 第一折
drop table if exists qyxs_ctr_jd_wy_folds1; 
create table qyxs_ctr_jd_wy_folds1
as
select f2.user_id, f2.jd_no,
f1.wy_sat_click_ratio,
f1.wy_dev_click_ratio
from
(select jd_no, bin_wy,
avg(satisfied)as wy_sat_click_ratio, 
avg(delivered) as wy_dev_click_ratio
from
qyxs_5_folds_jd_wy where folds!=1 group by jd_no, bin_wy) f1
right join
(select user_id, jd_no, bin_wy from qyxs_5_folds_jd_wy where folds=1) f2
on f2.jd_no=f1.jd_no and f2.bin_wy=f1.bin_wy;


-- 第二折
drop table if exists qyxs_ctr_jd_wy_folds2; 
create table qyxs_ctr_jd_wy_folds2
as
select f2.user_id, f2.jd_no,
f1.wy_sat_click_ratio,
f1.wy_dev_click_ratio
from
(select jd_no, bin_wy,
avg(satisfied)as wy_sat_click_ratio, 
avg(delivered) as wy_dev_click_ratio
from
qyxs_5_folds_jd_wy where folds!=2 group by jd_no, bin_wy) f1
right join
(select user_id, jd_no, bin_wy from qyxs_5_folds_jd_wy where folds=2) f2
on f2.jd_no=f1.jd_no and f2.bin_wy=f1.bin_wy;

-- 第三折
drop table if exists qyxs_ctr_jd_wy_folds3; 
create table qyxs_ctr_jd_wy_folds3
as
select f2.user_id, f2.jd_no,
f1.wy_sat_click_ratio,
f1.wy_dev_click_ratio
from
(select jd_no, bin_wy,
avg(satisfied)as wy_sat_click_ratio, 
avg(delivered) as wy_dev_click_ratio
from
qyxs_5_folds_jd_wy where folds!=3 group by jd_no, bin_wy) f1
right join
(select user_id, jd_no, bin_wy from qyxs_5_folds_jd_wy where folds=3) f2
on f2.jd_no=f1.jd_no and f2.bin_wy=f1.bin_wy;


-- 第四折 
drop table if exists qyxs_ctr_jd_wy_folds4; 
create table qyxs_ctr_jd_wy_folds4
as
select f2.user_id, f2.jd_no,
f1.wy_sat_click_ratio,
f1.wy_dev_click_ratio
from
(select jd_no, bin_wy,
avg(satisfied)as wy_sat_click_ratio, 
avg(delivered) as wy_dev_click_ratio
from
qyxs_5_folds_jd_wy where folds!=4 group by jd_no, bin_wy) f1
right join
(select user_id, jd_no, bin_wy from qyxs_5_folds_jd_wy where folds=4) f2
on f2.jd_no=f1.jd_no and f2.bin_wy=f1.bin_wy;

-- 第五折
drop table if exists qyxs_ctr_jd_wy_folds5; 
create table qyxs_ctr_jd_wy_folds5
as
select f2.user_id, f2.jd_no,
f1.wy_sat_click_ratio,
f1.wy_dev_click_ratio
from
(select jd_no, bin_wy,
avg(satisfied)as wy_sat_click_ratio, 
avg(delivered) as wy_dev_click_ratio
from
qyxs_5_folds_jd_wy where folds!=5 group by jd_no, bin_wy) f1
right join
(select user_id, jd_no, bin_wy from qyxs_5_folds_jd_wy where folds=5) f2
on f2.jd_no=f1.jd_no and f2.bin_wy=f1.bin_wy;


-- 合并五折数据并填充空值为-1
drop table if exists qyxs_ctr_wy_sub_train; 
create table qyxs_ctr_wy_sub_train
as
select t.user_id, t.jd_no, 
COALESCE(wy_sat_click_ratio, -1) as wy_sat_click_ratio,
COALESCE(wy_dev_click_ratio, -1) as wy_dev_click_ratio
from
(
select * from qyxs_ctr_jd_wy_folds1
UNION ALL 
select * from qyxs_ctr_jd_wy_folds2
UNION ALL 
select * from qyxs_ctr_jd_wy_folds3
UNION ALL 
select * from qyxs_ctr_jd_wy_folds4
UNION ALL 
select * from qyxs_ctr_jd_wy_folds5
) t;

-------------------------------------------- test ----------------------------------------------------
drop table if exists qyxs_ctr_wy_sub_test; 
create table qyxs_ctr_wy_sub_test
as
select f2.user_id, f2.jd_no,
COALESCE(f1.sat_click_ratio, -1) as wy_sat_click_ratio,
COALESCE(f1.dev_click_ratio, -1) as wy_dev_click_ratio
from
(select jd_no, bin_wy, 
avg(satisfied)as sat_click_ratio, 
avg(delivered) as dev_click_ratio
from
qyxs_5_folds_jd_wy group by jd_no, bin_wy) f1
right join
(select user_id, jd_no, bin_wy from qyxs_tmp_bin_work_years where satisfied=-1) f2
on f2.jd_no=f1.jd_no and f2.bin_wy=f1.bin_wy;