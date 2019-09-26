-- 去重并构造基础user和jd的交互特征
-- jd_cnt: 每个user投递的jd数, 包括重复
-- jd_nunique: 每个user投递的jd数, 不包括重复
-- user_jd_cnt: 每个user对同一个jd投递的次数
drop table if exists juz_uj_train_distinct; 
create table juz_uj_train_distinct 
as
select 
    tr.user_id,
    tr.jd_no, 
    tr.delivered, 
    tr.satisfied, 
    tr.user_jd_cnt,
    tr.jd_cnt, 
    tr.jd_nunique
from
(select t1.*, 
count(1) OVER(PARTITION BY t1.user_id, t1.jd_no) as user_jd_cnt,
count(t1.jd_no) OVER(PARTITION BY t1.user_id) as jd_cnt,
count(distinct t1.jd_no) OVER(PARTITION BY t1.user_id) as jd_nunique,
ROW_NUMBER() OVER(PARTITION BY t1.user_id, t1.jd_no ORDER BY t1.delivered desc, t1.satisfied desc) AS num
from 
(select user_id, jd_no,
 cast(delivered as int) delivered,
 cast(satisfied as int) satisfied
from otto_zhaopin_round2_action_train 
) t1) tr
WHERE tr.num=1;


drop table if exists juz_uj_test_distinct; 
create table juz_uj_test_distinct 
as
select distinct te.user_id, te.jd_no,  te.user_jd_cnt, te.jd_cnt, te.jd_nunique,
-1 as satisfied, -1 as delivered
from
(select *, 
count(1) OVER(PARTITION BY user_id, jd_no) as user_jd_cnt,
count(jd_no) OVER(PARTITION BY user_id) as jd_cnt,
count(distinct jd_no) OVER(PARTITION BY user_id) as jd_nunique
from otto_zhaopin_round2_action_test) te;