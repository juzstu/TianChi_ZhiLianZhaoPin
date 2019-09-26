-- 生成user基础信息表
drop table if exists qyxs_user_base_feat; 
create table qyxs_user_base_feat
as 
select user_id, desire_jd_industry_id, desire_jd_type_id,
cur_industry_id, cur_jd_type, 
case 
    when experience is null or experience = 'null' then 'empty'
    else experience end as experience,
size(split(replace(desire_jd_city_id, '-', ''), ',')) as desire_jd_city_cnt,
cast(split(replace(desire_jd_city_id, '-', ''), ',')[0] as int) as desire_jd_first_city,
case 
    when live_city_id != '-' then 
    decode(find_in_set(live_city_id, desire_jd_city_id)>0, true, 1, false, 0) 
    else -1 end as live_city_in_desire_city,
case 
    when live_city_id = '-' then -1
    else
    cast(live_city_id as int) end as live_city_id,
case when cur_degree_id='其他' then 0
     when cur_degree_id='初中' then 1
     when cur_degree_id='中技' or cur_degree_id='中专'  or cur_degree_id='高中' then 2
     when cur_degree_id='大专' then 3
     when cur_degree_id='本科' then 4
     when cur_degree_id='硕士' or cur_degree_id='MBA' or cur_degree_id='EMBA' then 5
     when cur_degree_id='博士' then 6
     else -1 end as degree_map,
case 
    when start_work_date='-' then NULL
    when SUBSTR(start_work_date, 5, 2)='00' then
    datediff('2019-08-01 00:00:00', CONCAT_WS('-', SUBSTR(start_work_date,1,4), '01-01 00:00:00') , 'year')
    else datediff('2019-08-01 00:00:00', CONCAT_WS('-', SUBSTR(start_work_date,1,4), SUBSTR(start_work_date, 5, 2), '01 00:00:00') , 'year')
    end as work_years,
cast(case when birthday != '-' then birthday else -1 end as int) as birthday,
SIZE(SPLIT(desire_jd_industry_id, ',')) as desire_jd_cnt,
SIZE(SPLIT(desire_jd_type_id, ',')) as desire_jd_type_cnt,
SIZE(SPLIT(experience, '\\|')) as exp_len,
cast(
case 
    when LENGTH(desire_jd_salary_id)==12 then SUBSTR(desire_jd_salary_id, 1, 6) 
    when LENGTH(desire_jd_salary_id)==10 then SUBSTR(desire_jd_salary_id, 1, 5) 
    when LENGTH(desire_jd_salary_id)==11 then SUBSTR(desire_jd_salary_id, 1, 5) 
    when LENGTH(desire_jd_salary_id)==9 then SUBSTR(desire_jd_salary_id, 1, 4) 
    else -1 end as int) as desire_jd_min_salary,
cast(
case 
    when LENGTH(desire_jd_salary_id)==12 then SUBSTR(desire_jd_salary_id, 7, 6) 
    when LENGTH(desire_jd_salary_id)==10 then SUBSTR(desire_jd_salary_id, 6, 5) 
    when LENGTH(desire_jd_salary_id)==11 then SUBSTR(desire_jd_salary_id, 6, 5) 
    when LENGTH(desire_jd_salary_id)==9 then SUBSTR(desire_jd_salary_id, 5, 4) 
    else -1 end as int) as desire_jd_max_salary,
cast(
case 
    when LENGTH(cur_salary_id)==12 then SUBSTR(cur_salary_id, 1, 6) 
    when LENGTH(cur_salary_id)==10 then SUBSTR(cur_salary_id, 1, 5) 
    when LENGTH(cur_salary_id)==11 then SUBSTR(cur_salary_id, 1, 5) 
    when LENGTH(cur_salary_id)==9 then SUBSTR(cur_salary_id, 1, 4) 
    else -1 end as int) as cur_min_salary,
cast(
case 
    when LENGTH(cur_salary_id)==12 then SUBSTR(cur_salary_id, 7, 6) 
    when LENGTH(cur_salary_id)==10 then SUBSTR(cur_salary_id, 6, 5) 
    when LENGTH(cur_salary_id)==11 then SUBSTR(cur_salary_id, 6, 5) 
    when LENGTH(cur_salary_id)==9 then SUBSTR(cur_salary_id, 5, 4) 
    else -1 end as int) as cur_max_salary,
count(distinct user_id) over(PARTITION by cur_industry_id) as pt_user_cid_cnt,
count(distinct user_id) over(PARTITION by live_city_id, cur_industry_id) as pt_user_cid_city_cnt,
count(distinct user_id) over(PARTITION by cur_jd_type) as pt_user_ctype_cnt,
count(distinct user_id) over(PARTITION by live_city_id, cur_jd_type) as pt_user_ctype_city_cnt
from otto_zhaopin_round2_user;