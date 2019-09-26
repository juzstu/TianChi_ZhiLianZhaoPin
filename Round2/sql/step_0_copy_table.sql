CREATE TABLE IF NOT EXISTS otto_zhaopin_round2_user AS
SELECT  *
FROM    odps_tc_257100_f673506e024.zhaopin_round2_user
;

CREATE TABLE IF NOT EXISTS otto_zhaopin_round2_jd AS
SELECT  *
FROM    odps_tc_257100_f673506e024.zhaopin_round2_jd
;

CREATE TABLE IF NOT EXISTS otto_zhaopin_round2_action_train AS
SELECT  *
FROM    odps_tc_257100_f673506e024.zhaopin_round2_action_train
;

CREATE TABLE IF NOT EXISTS otto_zhaopin_round2_action_test AS
SELECT  *
FROM    odps_tc_257100_f673506e024.zhaopin_round2_action_test
;
