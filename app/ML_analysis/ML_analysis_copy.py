import random

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV


class ML_run:
    def __init__(self, input_data, target_date):
        self.input_data = input_data
        self.target_date = target_date

    def ML(self, research_dict):
        input_data = self.input_data
        target_date = self.target_date
        # select target input
        results_df = pd.DataFrame()

        for i, v in list(input_data.items()):
            real_input = input_data[i]
            print(i, '월 분석이 진행 중입니다.')
            # ---------------------- 환자 단위 데이터 split -----------------------------------
            count_list = real_input['RN_INDI'].unique().tolist()
            random.shuffle(count_list)
            train_count = count_list[:int(len(count_list) * (2 / 3))]  # 2/3은 train set에
            test_count = count_list[int(len(count_list) * (2 / 3)):]  # 나머지 1/3 test set에 할당함
            train_count_df = pd.DataFrame(train_count, columns=['RN_INDI'])
            test_count_df = pd.DataFrame(test_count, columns=['RN_INDI'])

            train_df = pd.merge(real_input, train_count_df, on='RN_INDI', how='inner')
            test_df = pd.merge(real_input, test_count_df, on='RN_INDI', how='inner')

            train_df = train_df.groupby(['RN_INDI', 'case']).head(1)
            test_df = test_df.groupby(['RN_INDI', 'case']).head(1)

            y_train = train_df[['case']]
            x_train = train_df.drop(columns=['RN_INDI', 'case', 'VCNYMD'])

            y_test = test_df[['case']]
            x_test = test_df.drop(columns=['RN_INDI', 'case', 'VCNYMD'])

            # ---model running---#
            # grid search start
            # grid search란 머신러닝에서 필요한 Parameter들을 조합하여 가장 최적의 조합을 찾아 주는 기능입니다
            # 찾아보니 weka라는 것을 활용하면 자바에서도 머신러닝이 가능한듯 한데..
            # (https://www.codingame.com/playgrounds/7163/machine-learning-with-java---part-6-random-forest)
            # (https://griddb.net/en/blog/how-to-implement-a-random-forest-algorithm-in-java/)
            # (http://haifengl.github.io/api/java/smile/regression/RandomForest.html)
            # grid search가 가능한지는 잘 모르겠습니다.
            # 한 번 보시고, 불가능하실 것 같으면 알려주세요. 방법을 좀 더 찾아보겠습니다.

            # setting할 parameter들을 나열함 (parameter명은 파이썬 패키지 내 정해져있는 이름입니다. Java에서 가능한 모듈을 찾아서 알려주시면 제가 보고 parameter명을 알려드리겠습니다.)
            params_grid = {
                'n_estimators': [100, 200, 300],
                'max_depth': [10, 15, 20, 25],
                'criterion': ('gini', 'entropy')
            }

            model = RandomForestClassifier()  # random forest 모델을 불러옵니다.
            grid_search = GridSearchCV(
                estimator=model,
                param_grid=params_grid,
                scoring='roc_auc',
                cv=5
            )  # auroc를 기준으로 모델에서 최적의 parameter를 찾습니다
            grid_search.fit(x_train, y_train)
            model = grid_search.best_estimator_  # 가장 최적의 성능을 내는 best_estimator를 학습기로 활용합니다.

            y_pred_rf_train = model.predict_proba(x_train)[:, 1]
            y_pred_rf_test = model.predict_proba(x_test)[:, 1]

            # 머신러닝 예측 결과로 각 변수 별 중요도를 보는 feature_importance를 활용합니다.
            # 이는 내장된 파이썬 모듈을 사용합니다.
            # 위 링크 중 (http://haifengl.github.io/api/java/smile/regression/RandomForest.html) 를 보니 importance라는 것이 있기는 합니다. 확인 부탁드립니다
            features = pd.DataFrame(
                model.feature_importances_.reshape(-1, 1),
                index=x_train.columns
            ).sort_values(0, ascending=False)
            features = features.rename(columns={0: 'Feature_importance'})

            # 구해진 feature importance를 전체 변수의 feature importance로 나누어줍니다.
            features_int = ((features[['Feature_importance']] * 1000).astype(int))
            features_int_Nonzero = features_int[features_int['Feature_importance'] != 0]
            features_int_RR = (features_int_Nonzero / features_int_Nonzero['Feature_importance'].mean())
            shrink_cols = features_int_RR[features_int_RR['Feature_importance'] > 0].index.tolist()

            if 'VCN' not in shrink_cols:
                vcn_df = pd.DataFrame({'Feature_importance': 0}, index=['VCN'])
                features_int_RR = pd.concat([features_int_RR, vcn_df])

            fi_ratio = float(features_int_RR.loc['VCN'])
            injected_case = real_input[i].VCN.sum()
            risk_case = real_input[i].case.sum()
            calculated_date = str(i)

            # TODO: 유정샘 여기도 확인해주세요 입력받은 값이 들어오는게 아닌거같습니다.
            vaccine = research_dict.vaccine_target_id
            vcntme = research_dict.vcntime
            hoidefn = research_dict.hoidefn
            studydesign = research_dict.studydesignid

            results = ({
                'studydesign': [studydesign],
                'vaccine': [vaccine],
                'vcntime': [vcntme],
                'hoidefn': [hoidefn],
                'calculated_date': [calculated_date],
                'fi_ratio': [fi_ratio],
                'injected_case': [injected_case],
                'risk_case': [risk_case],
            })
            results_df = results_df.append(results, ignore_index=True)
            print(results_df)
            print(i, '월 분석이 완료 되었습니다.')

        return results_df  # 이 값이 AnalysisResultML에 들어갑니다.
