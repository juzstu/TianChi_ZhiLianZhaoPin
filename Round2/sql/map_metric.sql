select sat_map_score,del_map_score,0.7*sat_map_score+0.3*del_map_score as map_score from 
(select avg(a3.map_score) as sat_map_score
from
(SELECT AVG(a2.score) as map_score
 from (select user_id, 
       ROW_NUMBER() OVER(PARTITION BY a1.user_id ORDER BY a1.rank_value)/ a1.rank_value as score
       from(select user_id, jd_no, satisfied,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY cast(get_json_object(prediction_detail,"$.1") as DOUBLE) desc) AS rank_value
            from ${t1}) a1 
        WHERE a1.satisfied=1) a2 group by a2.user_id) a3) b1,
(select avg(a3.map_score) as del_map_score
from
(SELECT AVG(a2.score) as map_score
 from (select user_id, 
       ROW_NUMBER() OVER(PARTITION BY a1.user_id ORDER BY a1.rank_value)/ a1.rank_value as score
       from(select user_id, jd_no, delivered,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY 
            cast(get_json_object(prediction_detail,"$.1") as DOUBLE) desc) AS rank_value
            from ${t1}) a1 
        WHERE a1.delivered=1) a2 group by a2.user_id) a3) b2
