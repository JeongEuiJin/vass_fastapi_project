# main.py #
import warnings
from datetime import datetime

import pandas as pd

from app.ML_analysis.ML_analysis_copy import ML_run
from app.ML_analysis.duration_final import CaseDuration, case_groupby, ControlDuration
from app.ML_analysis.extract_final import extract_ml_input, extract_window
from app.ML_analysis.join_final import Join

warnings.filterwarnings("ignore")


def ml_run(research_dict, table_HOI, vac, bfc, table20, table30, table60, GNL2ATC):
    atc_n = 4  # 고정

    # dictionary reading
    use_start_date = int(research_dict.use_start_date)
    use_end_date = int(research_dict.use_end_date)
    research_start_date = int(research_dict.research_start_date)
    research_end_date = int(research_dict.research_end_date)
    window_size = int(research_dict.ml_window_size)
    age_type = research_dict.age_type
    age_select_start = int(research_dict.age_select_start)
    age_select_end = int(research_dict.age_select_end)

    # READ DATA
    HOI_patients = table_HOI.RN_INDI.tolist()

    print('read_data is done')

    vac = vac[['RN_INDI', 'VCNYMD', 'VCNCD', 'VCNTME']]
    vac = vac[vac['RN_INDI'].isin(HOI_patients)]
    vac.reset_index(inplace=True, drop=True)
    vac['VCNYMD'] = vac['VCNYMD'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))

    bfc = bfc[bfc['RN_INDI'].isin(HOI_patients)]
    bfc['birthdate'] = bfc['birthdate'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))
    bfc.reset_index(inplace=True, drop=True)
    bfc = bfc.merge(vac[['RN_INDI', 'VCNYMD']], how='left', on='RN_INDI')

    # TODO 만약 백신 데이터가 없는 경우 프린트 되는 알람입니다.
    if bfc.empty:
        print('좀 더 긴 관찰 기간을 설정해 주세요.')
    try:

        bfc['age_calcul'] = (bfc['VCNYMD'] - bfc['birthdate']).dt.days
        bfc['WEEK'] = bfc['age_calcul'] / 7
        bfc['AGE'] = (bfc['age_calcul'] / 365.25) + 1
        bfc['MONTH'] = bfc['age_calcul'] / 30.5

        if age_type == 'AGE':
            bfc = bfc.query('{}<=AGE<={}'.format(age_select_start, age_select_end))
        elif age_type == 'MONTH':
            bfc = bfc.query('{}<=MONTH<={}'.format(age_select_start, age_select_end))
        elif age_type == 'WEEK':
            bfc = bfc.query('{}<=WEEK<={}'.format(age_select_start, age_select_end))
        else:
            # TODO : 유정샘 bfc 없으면 에러나오게 하는걸 추가해야될거같네요
            pass

        # TODO 반영해서 추가한 부분입니다. 빈 bfc의 경우는 아래 문구가 프린트 됩니다.
        if bfc.empty:
            print('나이 설정을 다시 해 주세요.')

        # 조건을 만족하는 사람들의 table_HOI 기록을 추출하여 table_HOI로 저장하고 아래 분석 코드를 돌린다.
        bfc_patients = bfc.RN_INDI.tolist()
        table_HOI = table_HOI[table_HOI['RN_INDI'].isin(bfc_patients)]
        table_HOI.reset_index(inplace=True, drop=True)

        table20 = table20[table20.RN_INDI.isin(HOI_patients)]
        table20 = table20[['RN_KEY', 'RN_INDI', 'MDCARE_STRT_DT']]

        table30 = table30[['RN_KEY', 'MDCARE_STRT_DT', 'MCARE_DIV_CD']]
        table30.rename(columns={'MDCARE_STRT_DT': 'drug_date'}, inplace=True)
        table30 = table30.merge(table20, how='inner', on='RN_KEY')
        table30.rename(columns={'MCARE_DIV_CD': 'GNL_NM_CD'}, inplace=True)

        table60 = table60[['RN_KEY', 'MDCARE_STRT_DT', 'GNL_NM_CD']]
        table60.rename(columns={'MDCARE_STRT_DT': 'drug_date'}, inplace=True)
        table60 = table60.merge(table20, how='inner', on='RN_KEY')

        target_total_pre = pd.concat([table30, table60])  # table30, 60 전처리 완료된 파일 합치기
        del (table30)
        del (table60)
        del (table20)
        print('target_total_pre is done')

        # ATC코드라는 컬럼이 총 6-8자리로 이루어진 컬럼인데 앞의 4자리만 활용하기 위해 따로 떼어서 ATC_N 컬럼으로 분리함
        GNL2ATC['ATC_N'] = GNL2ATC['ATC'].str[:atc_n]

        # CASE / CONTROL DURATION #
        gap_delta = int(research_dict.gap_era)
        target_diag_duration = case_groupby(table_HOI, gap_delta)
        target_diag_duration.reset_index(inplace=True, drop=True)

        # 아래에서 window_size는 [연구디자인 생성 - machine learning - 위험 및 대조구간 길이] 입니다
        target_date = CaseDuration(target_diag_duration, window_size).target_case_duration()
        print('case extraction is done')
        # 아래에서 window_size는 [연구디자인 생성 - machine learning - 위험 및 대조구간 길이] 입니다
        # step_day 는 0으로 고정입니다.
        target_cont_duration = ControlDuration(
            target_diag_duration,
            research_end_date,
            window_size,
            step_day=0
        ).target_cont_duration()

        target_total = pd.concat([target_date, target_cont_duration])  # 위에서 만든 두 개의 테이블을 합침
        target_total.reset_index(inplace=True, drop=True)
        target_total_pre.reset_index(inplace=True, drop=True)
        print('ctrl extraction is done')
        # 각 구간 동안의 처방 데이터 추출
        drug_output = extract_ml_input(target_total_pre, target_total, GNL2ATC)
        print('drug_output is done')
        # 만들어진 데이터셋에 bfc, vac 테이블을 Join 합니다.
        input_df = Join(target_total, vac, drug_output, bfc, age_type).BFC()

        # # extract window
        input_data = extract_window(input_df, age_type)
        print('ML input is done')
        # # Run machine learning
        print('Whole process is done')
        result = ML_run(input_data, target_date).ML(research_dict)
        return result
    except:
        pass
    # return result

# if __name__ == '__main__':
#     table_HOI, vac, bfc, table20, table30, table60, GNL2ATC = sc.connect_db(research_dict)
#     print('DB reading is done.')
#
#     results = ml_run(research_dict, table_HOI, vac, bfc, table20, table30, table60, GNL2ATC)
#     print(results)
#    # results를 AnalysisResultsML table에 넣음
