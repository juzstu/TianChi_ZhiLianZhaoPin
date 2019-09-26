-- 合并所有ctr特征
drop table if exists qyxs_ctr_sub_train; 
create table qyxs_ctr_sub_train
as
select t3.*, 
t4.wy_sat_click_ratio,
t4.wy_dev_click_ratio
from
(select t1.*, 
t2.jdd_sat_click_ratio,
t2.jdd_dev_click_ratio
from qyxs_ctr_feats_sub_train t1
join
qyxs_ctr_jdd_sub_train t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no) t3
join qyxs_ctr_wy_sub_train t4
on t3.user_id=t4.user_id and t3.jd_no=t4.jd_no;


drop table if exists qyxs_ctr_sub_test; 
create table qyxs_ctr_sub_test
as
select t3.*, 
t4.wy_sat_click_ratio,
t4.wy_dev_click_ratio
from
(select t1.*, 
t2.jdd_sat_click_ratio,
t2.jdd_dev_click_ratio
from qyxs_ctr_feats_sub_no_avg_test t1
join
qyxs_ctr_jdd_sub_test t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no) t3
join qyxs_ctr_wy_sub_test t4
on t3.user_id=t4.user_id and t3.jd_no=t4.jd_no;


drop table if exists qyxs_merge_ctr_sub_train; 
create table qyxs_merge_ctr_sub_train
as
select t1.*, 
t2.jp_sat_click_cnt,
t2.jp_sat_all_cnt,
t2.jp_sat_click_ratio,
t2.jp_dev_click_cnt,
t2.jp_dev_click_ratio
from qyxs_ctr_sub_train t1
join
qyxs_ctr_jp_sub_train t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;


drop table if exists qyxs_merge_ctr_sub_test; 
create table qyxs_merge_ctr_sub_test
as
select t1.*, 
t2.jp_sat_click_cnt,
t2.jp_sat_all_cnt,
t2.jp_sat_click_ratio,
t2.jp_dev_click_cnt,
t2.jp_dev_click_ratio
from qyxs_ctr_sub_test t1
join
qyxs_ctr_jp_sub_test t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;


-- jd title
drop table if exists qyxs_final_ctr_sub_train; 
create table qyxs_final_ctr_sub_train
as
select t1.*, 
t2.jt_sat_click_cnt,
t2.jt_sat_all_cnt,
t2.jt_sat_click_ratio,
t2.jt_dev_click_cnt,
t2.jt_dev_click_ratio
from qyxs_merge_ctr_sub_train t1
join
qyxs_ctr_jt_sub_train t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;


drop table if exists qyxs_final_ctr_sub_test; 
create table qyxs_final_ctr_sub_test
as
select t1.*, 
t2.jt_sat_click_cnt,
t2.jt_sat_all_cnt,
t2.jt_sat_click_ratio,
t2.jt_dev_click_cnt,
t2.jt_dev_click_ratio
from qyxs_merge_ctr_sub_test t1
join
qyxs_ctr_jt_sub_test t2
on t1.user_id=t2.user_id and t1.jd_no=t2.jd_no;