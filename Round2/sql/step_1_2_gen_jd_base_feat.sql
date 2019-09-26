-- 生成jd基础信息表
drop table if exists qyxs_jd_base_feat; 
create table qyxs_jd_base_feat
as
select
t1.*, t1.jd_days/t1.require_nums as jd_per_in_days, 
case 
    when t1.min_salary=0 then -1
    else t1.max_salary/t1.min_salary end as jd_salary_div_max_min,
t1.require_nums*(t1.max_salary+t1.min_salary)/2 as jd_plan_cost,
avg(t1.max_salary) over(PARTITION by t1.jd_title) as pt_jt_avg_max_salary,
avg(t1.min_salary) over(PARTITION by t1.jd_title) as pt_jt_avg_min_salary,
avg(t1.max_salary) over(PARTITION by t1.jd_sub_type) as pt_js_avg_max_salary,
avg(t1.min_salary) over(PARTITION by t1.jd_sub_type) as pt_js_avg_min_salary,

avg(t1.require_nums) over(PARTITION by jd_title) as rn_jt_avg,
avg(t1.require_nums) over(PARTITION by city, jd_title) as rn_cjt_avg,
avg(t1.require_nums) over(PARTITION by jd_sub_type) as rn_js_avg,
avg(t1.require_nums) over(PARTITION by city, jd_sub_type) as rn_cjs_avg
from
(select jd_no, jd_title, jd_sub_type, 
case 
    when job_description='\\N' then 'empty'
    else job_description end as job_description,
cast(city as int) as city,
cast(is_travel as int) as is_travel,
cast(require_nums as int) as require_nums,
cast(max_salary as int) as max_salary,
cast(min_salary as int) as min_salary,
cast(SUBSTR(end_date, 5, 2) as int) as end_month,
cast(SUBSTR(end_date, 7, 2) as int) as end_day,
cast(SUBSTR(start_date, 5, 2) as int) as start_month,
cast(SUBSTR(start_date, 7, 2) as int) as start_day,
datediff(
    Concat(CONCAT_WS('-', SUBSTR(end_date,1,4), SUBSTR(end_date, 5, 2), SUBSTR(end_date, 7, 2)), ' 00:00:00') ,
    Concat(CONCAT_WS('-', SUBSTR(start_date,1,4), SUBSTR(start_date, 5, 2), SUBSTR(start_date, 7, 2)), ' 00:00:00'),
     'dd') as jd_days,
case when trim(min_edu_level)='其他' then 0
     when trim(min_edu_level)='初中' then 1
     when trim(min_edu_level)='中技' or trim(min_edu_level)='中专'  or trim(min_edu_level)='高中' then 2
     when trim(min_edu_level)='大专' then 3
     when trim(min_edu_level)='本科' then 4
     when trim(min_edu_level)='硕士' or trim(min_edu_level)='MBA' or trim(min_edu_level)='EMBA' then 5
     when trim(min_edu_level)='博士' then 6
     else -1 end as min_edu_level_map,
case 
    when min_years='103' then 1
    when min_years='305' then 3
    when min_years='510' then 5
    when min_years='1099' then 10
    else -1 end as min_work_year,
case 
    when min_years='103' then 3
    when min_years='305' then 5
    when min_years='510' then 10
    else -1 end as max_work_year,
case
    when jd_sub_type='\\N' then 0
    else 1 end as is_normal_jd_type,

-- 同一个jd_title下有多少份工作
-- 同一个城市、jd_title下有多少份工作
count(distinct jd_no) over(PARTITION by jd_title) as pt_jt_cnt,
count(distinct jd_no) over(PARTITION by city, jd_title) as pt_cjt_cnt,
count(distinct jd_no) over(PARTITION by jd_sub_type) as pt_js_cnt,
count(distinct jd_no) over(PARTITION by city, jd_sub_type) as pt_cjs_cnt
from otto_zhaopin_round2_jd) t1;