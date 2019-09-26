# -*- coding：utf-8 -*-
# Author: juzstu
# Time: 2019/8/22 0:31

import pandas as pd
import numpy as np
import jieba as jb
from sklearn.model_selection import KFold
import lightgbm as lgb
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
import re
import warnings
from tqdm import tqdm
from joblib import Parallel, delayed

warnings.filterwarnings('ignore')


def modified_jd_df(jd_path):
    tmp_list = []
    tmp_file = open(jd_path, encoding='utf8')
    for i, j in enumerate(tmp_file.readlines()):
        if i == 175425:
            j = j.replace('销售\t|置业顾问\t|营销', '销售|置业顾问|营销')
        tmp = j.split('\t')
        tmp_list.append(tmp)
    tmp_file.close()
    return pd.DataFrame(tmp_list[1:], columns=tmp_list[0])


def get_min_salary(x):
    if len(x) == 12:
        return int(x[:6])
    elif len(x) == 10:
        return int(x[:5])
    elif len(x) == 11:
        return int(x[:5])
    elif len(x) == 9:
        return int(x[:4])
    else:
        return -1


def get_max_salary(x):
    if len(x) == 12:
        return int(x[6:])
    elif len(x) == 10:
        return int(x[5:])
    elif len(x) == 11:
        return int(x[5:])
    elif len(x) == 9:
        return int(x[4:])
    else:
        return -1


def is_same_user_city(df):
    live_city_id = str(df['live_city_id'])
    desire_jd_city = df['desire_jd_city_id']
    return live_city_id in desire_jd_city


def jieba_cnt(df):
    experience = df['experience']
    jd_title = df['jd_title']
    jd_sub_type = df['jd_sub_type']
    if isinstance(experience, str) and isinstance(jd_sub_type, str):
        tmp_set = set(jb.cut_for_search(jd_title)) | set(jb.cut_for_search(jd_sub_type))
        experience = set(jb.cut_for_search(experience))
        tmp_cnt = 0
        for t in tmp_set:
            if t in experience:
                tmp_cnt += 1
        return tmp_cnt
    else:
        return 0


def cur_industry_in_desire(df):
    cur_industry_id = df['cur_industry_id']
    desire_jd_industry_id = df['desire_jd_industry_id']
    if isinstance(cur_industry_id, str) and isinstance(desire_jd_industry_id, str):
        return cur_industry_id in desire_jd_industry_id
    else:
        return -1


def desire_in_jd(df):
    desire_jd_type_id = df['desire_jd_type_id']
    jd_sub_type = df['jd_sub_type']
    if isinstance(jd_sub_type, str) and isinstance(desire_jd_type_id, str):
        return jd_sub_type in desire_jd_type_id
    else:
        return -1


def get_tfidf(df, names, merge_id):
    tfidf_enc_tmp = TfidfVectorizer(ngram_range=(1, 2))
    tfidf_vec_tmp = tfidf_enc_tmp.fit_transform(df[names])
    svd_tag_tmp = TruncatedSVD(n_components=10, n_iter=20, random_state=2019)
    tag_svd_tmp = svd_tag_tmp.fit_transform(tfidf_vec_tmp)
    tag_svd_tmp = pd.DataFrame(tag_svd_tmp)
    tag_svd_tmp.columns = [f'{names}_svd_{i}' for i in range(10)]
    return pd.concat([df[[merge_id]], tag_svd_tmp], axis=1)


def get_str(x):
    return ' '.join([i for i in jb.cut(x) if i not in stop_words])


def offline_eval_map(train_df, label, pred_col):
    tmp_train = train_df.copy()
    tmp_train['rank'] = tmp_train.groupby('user_id')[pred_col].rank(ascending=False, method='first')
    tmp_x = tmp_train[tmp_train[label] == 1]
    tmp_x[f'{label}_index'] = tmp_x.groupby('user_id')['rank'].rank(ascending=True, method='first')
    tmp_x['score'] = tmp_x[f'{label}_index'] / tmp_train['rank']
    return  tmp_x.groupby('user_id')['score'].mean().mean()


def sub_on_line(train_, test_, pred, label, cate_cols, is_shuffle=True, use_cate=True):
    print(f'data shape:\ntrain--{train_.shape}\ntest--{test_.shape}')
    n_splits = 5
    folds = KFold(n_splits=n_splits, shuffle=is_shuffle, random_state=1024)
    sub_preds = np.zeros((test_.shape[0], folds.n_splits))
    train_[f'{label}_pred'] = 0
    fold_importance_df = pd.DataFrame()
    fold_importance_df["Feature"] = pred
    print(f'Use {len(pred)} features ...')
    auc_scores = []
    params = {
        'learning_rate': 0.01,
        'boosting_type': 'gbdt',
        'objective': 'binary',
        'metric': 'auc',
        'num_leaves': 63,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'seed': 1,
        'bagging_seed': 1,
        'feature_fraction_seed': 7,
        'min_data_in_leaf': 20,
        'nthread': -1,
        'verbose': -1
    }
    train_user_id = train_['user_id'].unique()
    for n_fold, (train_idx, valid_idx) in enumerate(folds.split(train_user_id), start=1):
        print(f'the {n_fold} training start ...')
        train_x, train_y = train_.loc[train_['user_id'].isin(train_user_id[train_idx]), pred], train_.loc[
            train_['user_id'].isin(train_user_id[train_idx]), label]
        valid_x, valid_y = train_.loc[train_['user_id'].isin(train_user_id[valid_idx]), pred], train_.loc[
            train_['user_id'].isin(train_user_id[valid_idx]), label]
        print(f'for train user:{len(train_idx)}\nfor valid user:{len(valid_idx)}')
        if use_cate:
            dtrain = lgb.Dataset(train_x, label=train_y, categorical_feature=cate_cols)
            dvalid = lgb.Dataset(valid_x, label=valid_y, categorical_feature=cate_cols)
        else:
            dtrain = lgb.Dataset(train_x, label=train_y)
            dvalid = lgb.Dataset(valid_x, label=valid_y)

        clf = lgb.train(
            params=params,
            train_set=dtrain,
            num_boost_round=10000,
            valid_sets=[dvalid],
            early_stopping_rounds=100,
            verbose_eval=100
        )
        sub_preds[:, n_fold - 1] = clf.predict(test_[pred], num_iteration=clf.best_iteration)
        auc_scores.append(clf.best_score['valid_0']['auc'])
        fold_importance_df[f'fold_{n_fold}_imp'] = clf.feature_importance()
        train_.loc[train_['user_id'].isin(train_user_id[valid_idx]), f'{label}_pred'] = \
            clf.predict(valid_x, num_iteration=clf.best_iteration)

    five_folds = [f'fold_{f}_imp' for f in range(1, n_splits + 1)]
    fold_importance_df['avg_imp'] = fold_importance_df[five_folds].mean(axis=1)
    fold_importance_df.sort_values(by='avg_imp', ascending=False, inplace=True)
    fold_importance_df[['Feature', 'avg_imp']].to_csv('feat_imp_base.csv', index=False, encoding='utf8')
    test_[label] = np.mean(sub_preds, axis=1)
    print('auc score', np.mean(auc_scores))
    return test_[['user_id', 'jd_no', label]], train_[['user_id', 'jd_no', f'{label}_pred', label]]


if __name__ == "__main__":
    min_work_year = {103: 1, 305: 3, 510: 5, 1099: 10}
    max_work_year = {103: 3, 305: 5, 510: 10}
    degree_map = {'其他': 0, '初中': 1, '中技': 2, '中专': 2, '高中': 2, '大专': 3, '本科': 4,
                  '硕士': 5, 'MBA': 5, 'EMBA': 5, '博士': 6}

    sub_path = './submit/'
    train_data_path = './zhaopin_round1_train_20190716/'
    test_data_path = './zhaopin_round1_test_20190716/'
    train_user = pd.read_csv(train_data_path + 'table1_user', sep='\t')
    train_user['desire_jd_city_id'] = train_user['desire_jd_city_id'].apply(lambda x: re.findall('\d+', x))
    train_user['desire_jd_salary_id'] = train_user['desire_jd_salary_id'].astype(str)
    train_user['min_desire_salary'] = train_user['desire_jd_salary_id'].apply(get_min_salary)
    train_user['max_desire_salary'] = train_user['desire_jd_salary_id'].apply(get_max_salary)
    train_user['min_cur_salary'] = train_user['cur_salary_id'].apply(get_min_salary)
    train_user['max_cur_salary'] = train_user['cur_salary_id'].apply(get_max_salary)
    train_user.drop(['desire_jd_salary_id', 'cur_salary_id'], axis=1, inplace=True)
    train_jd = pd.read_csv(train_data_path + 'table2_jd.csv', sep='\t')
    train_jd.drop(['company_name', 'max_edu_level', 'is_mangerial', 'resume_language_required'], axis=1, inplace=True)

    train_jd['min_work_year'] = train_jd['min_years'].map(min_work_year)
    train_jd['max_work_year'] = train_jd['min_years'].map(max_work_year)
    train_jd['start_date'].replace(r'\N', '22000101', inplace=True)
    train_jd['end_date'].replace(r'\N', '22000101', inplace=True)
    train_jd['start_date'] = pd.to_datetime(train_jd['start_date'].astype(str).apply(lambda x:
                                                                                     f'{x[:4]}-{x[4:6]}-{x[6:]}'))
    train_jd['end_date'] = pd.to_datetime(train_jd['end_date'].astype(str).apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}'))
    train_jd.loc[train_jd['end_date'] == '2200-01-01', ['start_date', 'end_date']] = np.nan

    stop_words = [i.strip() for i in open('中文停用词表.txt', 'r', encoding='utf8').readlines()]
    stop_words.extend(['\n', '\xa0', '\u3000', '\u2002'])
    tmp_cut = Parallel(n_jobs=-1)(delayed(get_str)(train_jd.loc[ind]['job_description\n'])
                                  for ind in tqdm(train_jd.index))

    tfidf_enc = TfidfVectorizer(ngram_range=(1, 2))
    tfidf_vec = tfidf_enc.fit_transform(tmp_cut)
    svd_tag = TruncatedSVD(n_components=10, n_iter=20, random_state=2019)
    tag_svd = svd_tag.fit_transform(tfidf_vec)
    tag_svd = pd.DataFrame(tag_svd)
    tag_svd.columns = [f'desc_svd_{i}' for i in range(10)]
    train_jd = pd.concat([train_jd, tag_svd], axis=1)

    train_action = pd.read_csv(train_data_path + 'table3_action', sep='\t')
    train_action['user_jd_cnt'] = train_action.groupby(['user_id', 'jd_no'])['jd_no'].transform('count').values
    train_action['jd_cnt'] = train_action.groupby(['user_id'])['jd_no'].transform('count').values
    train_action['jd_nunique'] = train_action.groupby(['user_id'])['jd_no'].transform('nunique').values
    train_action = train_action.drop_duplicates()
    train_action.sort_values(['user_id', 'jd_no', 'delivered', 'satisfied'], inplace=True)
    train_action = train_action.drop_duplicates(subset=['user_id', 'jd_no'], keep='last')
    train_action = train_action[train_action['jd_no'].isin(train_jd['jd_no'].unique())]

    train = train_action.merge(train_user, on='user_id', how='left')
    train = train.merge(train_jd, on='jd_no', how='left')
    del train['browsed']

    print('train data base feats already generated ...')

    test_user = pd.read_csv(test_data_path + 'user_ToBePredicted', sep='\t')
    test_user['desire_jd_city_id'] = test_user['desire_jd_city_id'].apply(lambda x: re.findall('\d+', x))
    test_user['desire_jd_salary_id'] = test_user['desire_jd_salary_id'].astype(str)
    test_user['min_desire_salary'] = test_user['desire_jd_salary_id'].apply(get_min_salary)
    test_user['max_desire_salary'] = test_user['desire_jd_salary_id'].apply(get_max_salary)
    test_user['min_cur_salary'] = test_user['cur_salary_id'].apply(get_min_salary)
    test_user['max_cur_salary'] = test_user['cur_salary_id'].apply(get_max_salary)
    test_user.drop(['desire_jd_salary_id', 'cur_salary_id'], axis=1, inplace=True)

    test = pd.read_csv(test_data_path + 'zhaopin_round1_user_exposure_B_20190819', sep=' ')
    test['user_jd_cnt'] = test.groupby(['user_id', 'jd_no'])['jd_no'].transform('count').values
    test['jd_cnt'] = test.groupby(['user_id'])['jd_no'].transform('count').values
    test['jd_nunique'] = test.groupby(['user_id'])['jd_no'].transform('nunique').values
    test = test.drop_duplicates()

    test['delivered'] = -1
    test['satisfied'] = -1

    test = test.merge(test_user, on='user_id', how='left')
    test = test.merge(train_jd, on='jd_no', how='left')

    print('test data base feats already generated ...')

    all_data = train.append(test, sort=False)

    all_data['jd_user_cnt'] = all_data.groupby(['jd_no'])['user_id'].transform('count').values
    all_data['same_user_city'] = all_data.apply(is_same_user_city, axis=1).astype(int)
    all_data['city'].fillna(-1, inplace=True)
    all_data['city'] = all_data['city'].astype(int)
    all_data['same_com_live'] = (all_data['city'] == all_data['live_city_id']).astype(int)
    all_data['min_edu_level'] = all_data['min_edu_level'].apply(lambda x: x.strip() if isinstance(x, str) else x)
    all_data['cur_degree_id'] = all_data['cur_degree_id'].apply(lambda x: x.strip() if isinstance(x, str) else x)
    all_data['min_edu_level_num'] = all_data['min_edu_level'].map(degree_map)
    all_data['cur_degree_id_num'] = all_data['cur_degree_id'].map(degree_map)
    all_data['same_edu'] = (all_data['min_edu_level'] == all_data['cur_degree_id']).astype(int)
    all_data['gt_edu'] = (all_data['cur_degree_id_num'] >= all_data['min_edu_level_num']).astype(int)
    all_data['min_desire_salary_num'] = (all_data['min_desire_salary'] <= all_data['min_salary']).astype(int)
    all_data['min_cur_salary_num'] = (all_data['min_cur_salary'] <= all_data['min_salary']).astype(int)

    all_data['max_desire_salary_num'] = (all_data['max_desire_salary'] <= all_data['max_salary']).astype(int)
    all_data['max_cur_salary_num'] = (all_data['max_cur_salary'] <= all_data['max_salary']).astype(int)
    all_data['same_desire_industry'] = all_data.apply(cur_industry_in_desire, axis=1).astype(int)
    all_data['same_jd_sub'] = all_data.apply(desire_in_jd, axis=1).astype(int)

    all_data['start_month'] = all_data['start_date'].dt.month
    all_data['start_day'] = all_data['start_date'].dt.day
    all_data['end_month'] = all_data['start_date'].dt.month
    all_data['end_day'] = all_data['start_date'].dt.day
    all_data['jd_days'] = (all_data['end_date'] - all_data['start_date']).dt.days

    all_data['user_work_year'] = 2019 - all_data['start_work_date'].replace('-', np.nan).astype(float)
    all_data['gt_min_year'] = (all_data['user_work_year'] > all_data['min_work_year']).astype(int)
    all_data['gt_max_year'] = (all_data['user_work_year'] > all_data['max_work_year']).astype(int)
    all_data['len_experience'] = all_data['experience'].apply(
        lambda x: len(x.split('|')) if isinstance(x, str) else np.nan)
    all_data['desire_jd_industry_id_len'] = all_data['desire_jd_industry_id'].apply(
        lambda x: len(x.split(',')) if isinstance(x, str) else np.nan)
    all_data['desire_jd_type_id_len'] = all_data['desire_jd_type_id'].apply(
        lambda x: len(x.split(',')) if isinstance(x, str) else np.nan)
    all_data['eff_exp_cnt'] = all_data.apply(jieba_cnt, axis=1)
    all_data['eff_exp_ratio'] = all_data['eff_exp_cnt'] / all_data['len_experience']
    all_data.drop(['cur_degree_id_num', 'cur_degree_id', 'desire_jd_city_id', 'min_years',
                   'start_work_date', 'start_date', 'end_date', 'key', 'min_edu_level'], axis=1, inplace=True)

    # 城市统计
    all_data['user_jd_city_nunique'] = all_data.groupby('user_id')['city'].transform('nunique').values
    all_data['jd_user_city_nunique'] = all_data.groupby('jd_no')['live_city_id'].transform('nunique').values

    all_data['jd_title_nunique'] = all_data.groupby('user_id')['jd_title'].transform('nunique').values
    all_data['jd_sub_type_nunique'] = all_data.groupby('user_id')['jd_sub_type'].transform('nunique').values

    all_data['user_desire_jd_industry_id_nunique'] = all_data.groupby('jd_no')['desire_jd_industry_id'].transform(
        'nunique').values
    all_data['user_desire_jd_type_id_nunique'] = all_data.groupby('jd_no')['desire_jd_type_id'].transform(
        'nunique').values

    # 薪资
    all_data['user_jd_min_salary_min'] = all_data.groupby('user_id')['min_salary'].transform('min').values
    all_data['user_jd_min_salary_max'] = all_data.groupby('user_id')['min_salary'].transform('max').values
    all_data['user_jd_min_salary_mean'] = all_data.groupby('user_id')['min_salary'].transform('mean').values
    all_data['user_jd_min_salary_std'] = all_data.groupby('user_id')['min_salary'].transform('std').values

    all_data['user_jd_max_salary_min'] = all_data.groupby('user_id')['max_salary'].transform('min').values
    all_data['user_jd_max_salary_max'] = all_data.groupby('user_id')['max_salary'].transform('max').values
    all_data['user_jd_max_salary_mean'] = all_data.groupby('user_id')['max_salary'].transform('mean').values
    all_data['user_jd_max_salary_std'] = all_data.groupby('user_id')['max_salary'].transform('std').values

    all_data['jd_user_desire_min_salary_min'] = all_data.groupby('jd_no')['min_desire_salary'].transform('min').values
    all_data['jd_user_desire_min_salary_max'] = all_data.groupby('jd_no')['min_desire_salary'].transform('max').values
    all_data['jd_user_desire_min_salary_mean'] = all_data.groupby('jd_no')['min_desire_salary'].transform('mean').values
    all_data['jd_user_desire_min_salary_std'] = all_data.groupby('jd_no')['min_desire_salary'].transform('std').values

    all_data['jd_user_desire_max_salary_min'] = all_data.groupby('jd_no')['max_desire_salary'].transform('min').values
    all_data['jd_user_desire_max_salary_max'] = all_data.groupby('jd_no')['max_desire_salary'].transform('max').values
    all_data['jd_user_desire_max_salary_mean'] = all_data.groupby('jd_no')['max_desire_salary'].transform('mean').values
    all_data['jd_user_desire_max_salary_std'] = all_data.groupby('jd_no')['max_desire_salary'].transform('std').values

    all_data['jd_days_min'] = all_data.groupby('user_id')['jd_days'].transform('min').values
    all_data['jd_days_max'] = all_data.groupby('user_id')['jd_days'].transform('max').values
    all_data['jd_days_mean'] = all_data.groupby('user_id')['jd_days'].transform('mean').values
    all_data['jd_days_std'] = all_data.groupby('user_id')['jd_days'].transform('std').values
    all_data['jd_days_skew'] = all_data.groupby('user_id')['jd_days'].transform('skew').values

    all_data['age_min'] = all_data.groupby('jd_no')['birthday'].transform('min').values
    all_data['age_max'] = all_data.groupby('jd_no')['birthday'].transform('max').values
    all_data['age_mean'] = all_data.groupby('jd_no')['birthday'].transform('mean').values
    all_data['age_std'] = all_data.groupby('jd_no')['birthday'].transform('std').values
    all_data['age_skew'] = all_data.groupby('jd_no')['birthday'].transform('skew').values

    for j in ['jd_title', 'jd_sub_type']:
        le = LabelEncoder()
        all_data[j].fillna('nan', inplace=True)
        all_data[f'{j}_map_num'] = le.fit_transform(all_data[j])

    all_data['experience'] = all_data['experience'].apply(lambda x: ' '.join(x.split('|') if
                                                                             isinstance(x, str) else 'nan'))
    exp_gp = all_data.groupby('jd_no')['experience'].agg(lambda x: ' '.join(x.to_list())).reset_index()
    exp_gp = get_tfidf(exp_gp, 'experience', 'jd_no')
    all_data = all_data.merge(exp_gp, on='jd_no', how='left')

    use_feats = [c for c in all_data.columns if c not in ['user_id', 'jd_no', 'delivered', 'satisfied'] +
                 ['desire_jd_industry_id', 'desire_jd_type_id', 'cur_industry_id', 'cur_jd_type', 'experience',
                 'jd_title', 'jd_sub_type', 'job_description\n']]

    sub_sat, train_pred_sat = sub_on_line(all_data[all_data['satisfied'] != -1], all_data[all_data['satisfied'] == -1],
                                          use_feats, 'satisfied', ['live_city_id', 'city'], use_cate=True)

    sub_dev, train_pred_dev = sub_on_line(all_data[all_data['delivered'] != -1], all_data[all_data['delivered'] == -1],
                                          use_feats, 'delivered', ['live_city_id', 'city'], use_cate=True)

    train_pred_sat['merge_pred'] = train_pred_sat['satisfied_pred'] * 0.8 + train_pred_dev['delivered_pred'] * 0.2
    sub_sat['merge_prob'] = sub_sat['satisfied'] * 0.8 + sub_dev['delivered'] * 0.2

    train_pred_sat = train_pred_sat.merge(all_data[all_data['delivered'] != -1][['user_id', 'jd_no', 'delivered']],
                                          on=['user_id', 'jd_no'], how='left')

    dev_map = offline_eval_map(train_pred_sat, 'delivered', 'merge_pred')
    sat_map = offline_eval_map(train_pred_sat, 'satisfied', 'merge_pred')
    print('dev map:', round(dev_map, 4), 'sat map:', round(sat_map, 4), 'final score:',
          round(0.7 * sat_map + 0.3 * dev_map, 4))

    sub_df = pd.DataFrame(columns=['user_id', 'jd_no', 'merge_prob'])
    for i in sub_sat['user_id'].unique():
        tmp_sub = sub_sat[(sub_sat['user_id'] == i) &
                            (sub_sat['jd_no'].isin(train_jd['jd_no']))].sort_values('merge_prob', ascending=False)[
                            ['user_id', 'jd_no', 'merge_prob']]
        sub_df = sub_df.append(tmp_sub)
        sub_df = sub_df.append(sub_sat[(sub_sat['user_id'] == i) & (~sub_sat['jd_no'].isin(train_jd['jd_no']))][
                                   ['user_id', 'jd_no', 'merge_rank']])
    sub_df[['user_id', 'jd_no']].to_csv('sub_base.csv', index=False)
