-- 生成user和jd交互特征，运行在pai实验里的step2_data_merge_gen_feats, 此处为备份
select *, 
count(user_id) OVER(PARTITION by jd_no) as jd_user_cnt,
count(user_id) OVER(PARTITION by jd_no)/require_nums as require_jd_ratio,
DECODE(live_city_id=city, true, 1, false, 0) as same_desire_live_city,
case 
    when min_edu_level_map=-1 or degree_map=-1 then -999
    else degree_map - min_edu_level_map end as edu_diff,
case
    when min_salary<=0 or desire_jd_min_salary<=0 then -999
    else desire_jd_min_salary/min_salary end as desire_salary_min_ratio,
case
    when min_salary<=0 or cur_min_salary<=0 then -999
    else cur_min_salary/min_salary end as cur_salary_min_ratio,
case
    when max_salary<=0 or desire_jd_max_salary<=0 then -999
    else desire_jd_max_salary/max_salary end as desire_salary_max_ratio,
case
    when max_salary<=0 or cur_max_salary<=0 then -999
    else cur_max_salary/max_salary end as cur_salary_max_ratio,
case
    when min_work_year<=0 or work_years<=0 then -999
    else min_work_year/work_years end as min_work_years_ratio,
case
    when max_work_year<=0 or work_years<=0 then -999
    else max_work_year/work_years end as max_work_years_ratio,

DECODE(find_in_set(desire_jd_industry_id, cur_industry_id)>0, true, 1, false, 0) as same_desire_industry,
DECODE(find_in_set(jd_sub_type, desire_jd_type_id)>0, true, 1, false, 0) as same_jd_sub,

count(distinct city) over(PARTITION by user_id) as user_jd_city_nunique,
count(distinct live_city_id) over(PARTITION by jd_no) as jd_user_city_nunique,

count(distinct jd_title) over(PARTITION by user_id) as jd_title_nunique,
count(distinct jd_sub_type) over(PARTITION by user_id) as jd_sub_type_nunique,
count(distinct desire_jd_industry_id) over(PARTITION by jd_no) as user_desire_jd_industry_id_nunique,
count(distinct desire_jd_type_id) over(PARTITION by jd_no) as user_desire_jd_type_id_nunique,

-- 新增 投递相同行业的user数量
count(distinct user_id) over(PARTITION by jd_title) as ut_jt_cnt,
count(distinct user_id) over(PARTITION by city, jd_title) as ut_cjt_cnt,

count(distinct user_id) over(PARTITION by jd_sub_type) as ut_js_cnt,
count(distinct user_id) over(PARTITION by city, jd_sub_type) as ut_cjs_cnt,

min(require_nums) over(PARTITION by user_id) as user_jd_require_nums_min,
max(require_nums) over(PARTITION by user_id) as user_jd_require_nums_max,
avg(require_nums) over(PARTITION by user_id) as user_jd_require_nums_mean,
stddev(require_nums) over(PARTITION by user_id) as user_jd_require_nums_std,

min(min_salary) over(PARTITION by user_id) as user_jd_min_salary_min,
max(min_salary) over(PARTITION by user_id) as user_jd_min_salary_max,
avg(min_salary) over(PARTITION by user_id) as user_jd_min_salary_mean,
stddev(min_salary) over(PARTITION by user_id) as user_jd_min_salary_std,

min(max_salary) over(PARTITION by user_id) as user_jd_max_salary_min,
max(max_salary) over(PARTITION by user_id) as user_jd_max_salary_max,
avg(max_salary) over(PARTITION by user_id) as user_jd_max_salary_mean,
stddev(max_salary) over(PARTITION by user_id) as user_jd_max_salary_std,

min(desire_jd_min_salary) over(PARTITION by jd_no) as jd_user_desire_min_salary_min,
max(desire_jd_min_salary) over(PARTITION by jd_no) as jd_user_desire_min_salary_max,
avg(desire_jd_min_salary) over(PARTITION by jd_no) as jd_user_desire_min_salary_mean,
stddev(desire_jd_min_salary) over(PARTITION by jd_no) as jd_user_desire_min_salary_std,

min(desire_jd_max_salary) over(PARTITION by jd_no) as jd_user_desire_max_salary_min,
max(desire_jd_max_salary) over(PARTITION by jd_no) as jd_user_desire_max_salary_max,
avg(desire_jd_max_salary) over(PARTITION by jd_no) as jd_user_desire_max_salary_mean,
stddev(desire_jd_max_salary) over(PARTITION by jd_no) as jd_user_desire_max_salary_std,

min(jd_days) over(PARTITION by user_id) as jd_days_min,
max(jd_days) over(PARTITION by user_id) as jd_days_max,
avg(jd_days) over(PARTITION by user_id) as jd_days_mean,
stddev(jd_days) over(PARTITION by user_id) as jd_days_std,

min(birthday) over(PARTITION by jd_no) as age_min,
max(birthday) over(PARTITION by jd_no) as age_max,
avg(birthday) over(PARTITION by jd_no) as age_mean,
stddev(birthday) over(PARTITION by jd_no) as age_std
from ${t1};