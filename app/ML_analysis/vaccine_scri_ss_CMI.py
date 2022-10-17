import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import statsmodels.api as sm
import statsmodels.formula.api as smf
import math

import warnings

warnings.filterwarnings('ignore')

def data_integrating(research_dict, table_HOI, bfc_total, infrm_dth, vac):
    use_start_date = int(research_dict.use_start_date)
    use_end_date = int(research_dict.use_end_date)

    table_HOI['MDCARE_STRT_DT'] = pd.to_datetime(table_HOI['MDCARE_STRT_DT'],
                                                 format='%Y%m%d')  # table_HOI MDCARE_STRT_DT 컬럼을 날짜형으로 형변환. %Y%m%d format은 20220801의 형식.
    table_HOI.rename(columns={'MDCARE_STRT_DT': 'diag_dt'},
                     inplace=True)  # table_HOI 의 MDCARE_STRT_DT 를 diag_dt 로 이름 바꾸기

    ## 환자 데이터 및 접종 데이터
    ## 자격 데이터 환자ID, 성별을 연도별로 중복 없이 병합
    # [연구디자인 생성 - 분석 대상 데이터 기간] 시작 연도와 끝 연도 사이의 bfc table 들만 bfc_name_list에 저장합니다.
    bfc_total = bfc_total[['RN_INDI', 'birthdate', 'SEX']]
    bfc_total.birthdate = bfc_total.birthdate.astype(
        int)  ############################### 추가 ***************************
    bfc_total.RN_INDI = bfc_total.RN_INDI.astype(int)  ############################### 추가 ***************************
    bfc_total = bfc_total.query('@use_start_date<=birthdate<=@use_end_date')
    bfc_total.birthdate = pd.to_datetime(bfc_total.birthdate, format='%Y%m%d')

    ## 사망 변수 연결
    infrm_dth = infrm_dth[infrm_dth.DTH_YYYYMM != 0]
    infrm_dth = infrm_dth.loc[:, ['RN_INDI', 'DTH_YYYYMM']]  # 전체 컬럼 중에 RN_INDI, DTH_YYYYMM 컬럼만 가져옴
    infrm_dth = infrm_dth.astype({'DTH_YYYYMM': 'str'})
    infrm_dth.DTH_YYYYMM = infrm_dth.DTH_YYYYMM + '01'  # 사망 연월밖에 없어서 임시로 01 붙혀서 1일이라고 지칭함 ********
    infrm_dth.drop_duplicates(['RN_INDI'], inplace=True)  # RN_INDI 컬럼 기준으로 중복된 행 제거
    infrm_dth.reset_index(drop=True, inplace=True)
    infrm_dth.DTH_YYYYMM = pd.to_datetime(infrm_dth.DTH_YYYYMM, format='%Y%m%d')  # DTH_YYYYMM 컬럼을 날짜형으로 형변환
    infrm_table = bfc_total.merge(infrm_dth, how='left',
                                  on='RN_INDI')  # bfc_total 에 RN_INDI 이 같은 infrm_dth 데이터를 병합하여 infrm_table 에 저장

    ## 백신 접종 환자id 및 접종일 병합
    vac = vac.loc[:, ['RN_INDI', 'VCNYMD', 'VCNCD', 'VCNTME']]  # vac의 전체 컬럼 중에 RN_INDI, VCNYMD, VCNTME, VCNCD 컬럼만 가져옴
    vac = vac[vac.VCNYMD != '20180229']  ############################### 추가 ***************************
    vac.VCNYMD = pd.to_datetime(vac.VCNYMD, format='%Y%m%d')  # VCNYMD 컬럼을 날짜형으로 형변환

    observ_strt = int(research_dict.research_start_date)  # research_dict에서 관찰기간 시작지점 가져옴*
    observ_end = int(research_dict.research_end_date)  # research_dict에서 관찰기간 끝지점 가져옴*
    observ_strt = pd.to_datetime(observ_strt, format='%Y%m%d')
    observ_end = pd.to_datetime(observ_end, format='%Y%m%d')

    # observ_strt 부터 observ_end 사이의 VCNYMD를 갖는 데이터만 추출
    vac = vac[(datetime.timedelta(days=0) <= (vac.VCNYMD - observ_strt)) & (
            (observ_end - vac.VCNYMD) >= datetime.timedelta(days=0))]

    TME_num = int(research_dict.vcntime)
    if TME_num != None:  # TME_num 즉, 차수 선택이 안된 경우는 if문이 돌아가지 않게 조건문 설정
        vac = vac[vac.VCNTME == int(TME_num)]  # 차수 선택 TME_num에 해당하는 vac 파일만을 저장

    vac.RN_INDI = vac.RN_INDI.astype(int)  ############################### 추가 ***************************
    vac_elig = infrm_table.merge(vac, how='left',
                                 on='RN_INDI')  # infrm_table 에 RN_INDI 이 같은 vac 데이터를 병합하여 vac_elig 에 저장함

    day_type = research_dict.age_type

    if day_type != 'EMPTY': # 분석 연령 탭 - 제한 없음이 아닐 경우
        day_strt = int(research_dict.age_select_start)
        day_end = int(research_dict.age_select_end)

        # day_type으로 받은 시간 단위를 기준으로, day_strt ~ day_end 사이의 나이에 해당하는 데이터 추출
        # 나이는 vac_elig의 VCNYMD - birthdate 결과
        # Ex. '주수' 선택, day_strt = 1, day_end = 3일 경우, 1주 ~ 3주 사이의 나이에 대항하는 데이터만 추출
        if day_type == 'WEEK':  # 주수의 경우 weeks를 기준으로
            vac_elig = vac_elig[(datetime.timedelta(weeks=day_strt) <= (vac_elig.VCNYMD - vac_elig.birthdate)) & (
                    (vac_elig.VCNYMD - vac_elig.birthdate) <= datetime.timedelta(weeks=day_end))]
        elif day_type == 'MONTH':  # 개월 수의 경우에는 입력받은 값에 31을 곱한 기준으로
            vac_elig = vac_elig[(datetime.timedelta(days=31 * day_strt) <= (vac_elig.VCNYMD - vac_elig.birthdate)) & (
                    (vac_elig.VCNYMD - vac_elig.birthdate) <= datetime.timedelta(days=31 * day_end))]
        elif day_type == 'YEAR':  # 나이 수의 경우에는 입력받은 값에 365.25를 곱한 기준으로
            vac_elig = vac_elig[(datetime.timedelta(days=365.25 * day_strt) <= (vac_elig.VCNYMD - vac_elig.birthdate)) & (
                    (vac_elig.VCNYMD - vac_elig.birthdate) <= datetime.timedelta(days=365.25 * day_end))]
        else:
            pass

    disease_1 = table_HOI[['RN_INDI', 'diag_dt']]
    disease_1.RN_INDI = disease_1.RN_INDI.astype(int)  ############################### 추가 ***************************
    vac_diag = pd.merge(left=disease_1, right=vac_elig, how="left",
                        on="RN_INDI")  # disease_1 에 RN_INDI 가 같은 vac_elig 데이터를 병합하여 vac_diag 에 저장함
    vac_diag = vac_diag.dropna(subset=['birthdate'])  # use_date 사이에 없는 출생 사람은 날림
    vac_diag.drop_duplicates(inplace=True)  ############################### 추가 ***************************
    vac_diag.reset_index(drop=True, inplace=True)

    return vac_diag


def scri_analysis_sex_type(research_dict, df_vac_diag, sex_type, strt_date, end_date):
    strt_date = pd.to_datetime(strt_date, format='%Y%m%d')
    end_date = pd.to_datetime(end_date, format='%Y%m%d')

    pr = pd.period_range(strt_date, end_date, freq='M')

    final_result = pd.DataFrame()

    for i in pr:
        date_range_strt = str(i)
        date_range = str(i) + '-01'
        date_range = pd.to_datetime(date_range, format='%Y-%m-%d')
        ## 여기서 디버깅했을때 위단의 함수에서 이미 만들어진 df_vac_diag를 보시면 진단 날짜가 백신 접종 날짜 보다 전부 뒤에 존재합니다...
        ## 백신 접종 날짜가 2017년부터만 있네요
        ################### 여기 ####################################
        df_vac_diag_range = df_vac_diag[
            (date_range <= df_vac_diag.VCNYMD) & (df_vac_diag.VCNYMD < date_range + relativedelta(months=1))]
        df_vac_diag_range.reset_index(drop=True, inplace=True)
        vac_count = len(df_vac_diag_range)

        df_vac_diag_range['gap'] = df_vac_diag_range['diag_dt'] - df_vac_diag_range[
            'VCNYMD']  # vac_diag 에 diag_dt 값 - VCNYMD 값에 대한 gap 컬럼 생성
        df_vac_diag_range = df_vac_diag_range[
            (datetime.timedelta(days=0) <= df_vac_diag_range.gap)]  # gap 값이 0 이상의 값을 가지는 경우만 가져옴
        df_vac_diag_range.reset_index(drop=True, inplace=True)  # vac_diag 의 index를 reset하기
        vac_diag1 = df_vac_diag_range.sort_values(
            by=["RN_INDI", "VCNYMD", "diag_dt"])  # vac_diag 행들을 RN_INDI, VCNYMD, diag_dt 순서로 오름차순 정렬 후 vac_diag1 에 저장

        # 접종 이후 가장 빠른 진단일 사이의 시간대 확인
        vac_diag2 = vac_diag1.loc[vac_diag1.groupby(['RN_INDI', 'VCNYMD'])[
            'diag_dt'].idxmin()]  # vac_diag1 중에서 RN_INDI, VCNYMD으로 group을 묶은 후, 각 group중에서 diag_dt 값이 최소인 행만 가져와 vac_diag2에 저장
        vac_diag2.reset_index(drop=True, inplace=True)  # vac_diag2 의 index를 reset하기

        ### 일 단위 기초 데이터셋 구축
        # Risk Window R1 ~ R2 일
        # Control Window C1 ~ C2 일
        # 임시로 R1,R2,C1,C2 1 14 36 49 사용
        R1 = int(research_dict.scri_risk_window_start)  # research_dict 에서 가져옴*
        R2 = int(research_dict.scri_risk_window_end)  # research_dict 에서 가져옴*
        C1 = int(research_dict.scri_con_window_start_1)  # research_dict 에서 가져옴*
        C2 = int(research_dict.scri_con_window_end_1)  # research_dict 에서 가져옴*
        C3 = research_dict.scri_con_window_start_2  # research_dict 에서 가져옴*
        C4 = research_dict.scri_con_window_end_2

        # Risk,Control window내에 사망 포함된 경우 제외
        if (C3 == None) or (C4 == None):  # C3, C4에 입력 값이 없을 경우 조건문
            Cmax = max(C1, C2)  # C1 ~ C2 중 최대 값을 Cmax 값에 저장
        else:  # C3, C4에 입력 값이 모두 있을 경우 조건문
            C3 = int(C3)  # research_dict 에서 가져옴*
            C4 = int(C4)  # research_dict 에서 가져옴*
            Cmax = max(C1, C2, C3, C4)  # C1 ~ C4 중 최대 값을 Cmax 값에 저장
        # vac_diag2 중에서 DTH_YYYYMM값이 없거나, DTH_YYYYMM 값이 VCNYMD + 날짜형의(Cmax) 보다 큰 경우의 데이터만 가져옴
        vac_diag3 = vac_diag2[(vac_diag2.DTH_YYYYMM.isnull()) |
                              (vac_diag2.DTH_YYYYMM > (vac_diag2.VCNYMD + datetime.timedelta(days=Cmax)))]
        vac_diag5 = vac_diag3.copy()

        del (vac_diag2)
        del (vac_diag3)

        # 백신 접종데이터에 Risk, Control window 일자를 추가한 data열 추가
        if (C3 == None) or (C4 == None):  # C3, C4에 입력 값이 없을 경우 조건문
            date_window = list(range(R1, R2 + 1)) + list(range(C1, C2 + 1))  # 위에서 지정한 R1 ~ R2, C1 ~ C2 두 구간의 모든 수들의 리스트 생성
        else:  # C3, C4에 입력 값이 모두 있을 경우 조건문
            C3 = int(C3)  # research_dict 에서 가져옴*
            C4 = int(C4)
            date_window = list(range(R1, R2 + 1)) + list(range(C1, C2 + 1)) + list(range(C3, C4 + 1))  # 위에서 지정한 R1 ~ R2, C1 ~ C2, C3 ~ C4 세 구간의 모든 수들의 리스트 생성
        day_window = vac_diag5[['RN_INDI', 'VCNYMD']]  # vac_diag5 에서 해당 컬럼들 데이터를 day_window에 지정

        # day_window.RN_INDI = day_window.RN_INDI.astype(str)
        # vac_diag5.RN_INDI = vac_diag5.RN_INDI.astype(str)
        day_window_1 = pd.DataFrame(np.repeat(day_window.values, len(date_window),
                                              axis=0))  # day_window의 각 행들을 date_window 리스트의 길이 만큼 반복하여 생성함 (길이가 10이면 5개의 행이 각각 10개씩 불어나서 총 50 길이의 데이터프레임이 됨)
        day_window_1.columns = day_window.columns  # repeat로 만든 day_window_1의 컬럼 이름을 day_window와 같게 지정함
        # day_window_1에 rep_window 컬럼을 2열에 추가 (꼭 2열이 아니어도 됨). rep_window 컬럼이 가지는 값은 date_window의 값들을 day_window의 길이만큼 반복해서 넣는다.
        # Ex. date_window 값이 123, day_window길이가 4일 경우, 행마다 1 2 3 1 2 3 1 2 3 1 2 3 순서대로
        day_window_1.insert(2, 'rep_window', date_window * len(day_window), True)
        # day_window_1이 만들어지는 repeat과정에서 RN_INDI과 VCNYMD의 형태가 float과 날짜형이 아니게 될 경우 아래의 형변환 진행
        day_window_1.RN_INDI = day_window_1.RN_INDI.astype(float)  # day_window_1 의 RN_INDI 을 float형으로 변환
        day_window_1.VCNYMD = pd.to_datetime(day_window_1.VCNYMD)  # day_window_1 의 VCNYMD 을 날짜형으로 변환

        # vac_diag5 에 RN_INDI, VCNYMD 가 같은 day_window_1 데이터를 병합하여 day 에 저장함
        # left join으로 병합되므로, vac_diag5의 한 개의 행이, RN_INDI, VCNYMD이 일치하는 day_window_1 데이터 길이 만큼 늘어남
        day = pd.merge(left=vac_diag5, right=day_window_1, how="left", on=["RN_INDI", "VCNYMD"])
        day['date'] = day.VCNYMD  # day의 date 컬럼을 생성. VCNYMD 컬럼의 값을 똑같이 가짐
        for i in range(len(day)):  # day의 길이만큼 for루프 진행
            if pd.isnull(day.rep_window[i])==True:
                pass
            else:
                day['date'][i] = day.VCNYMD[i] + datetime.timedelta(days=int(day.rep_window[i]))  # i번째 행의 date 컬럼 값에 (VCNYMD 컬럼값 + rep_window컬럼값)을 저장
        day = day.drop(['rep_window'], axis=1)  # rep_window 컬럼을 버림

        # date - VCNYMD인 riskperiod열 추가 (Risk=1, control=2)
        day['riskperiod'] = 0  # day에 riskperiod 컬럼을 생성하며 0 값을 가짐

        if (C3 == None) or (C4 == None):  # C3, C4에 입력 값이 없을 경우 조건문
            for i in range(len(day)):  # day의 길이만큼 for 루프 진행
                if (datetime.timedelta(days=R1) <= day.date[i] - day.VCNYMD[i] <= datetime.timedelta(
                        days=R2)):  # i번째 date-VCNYMD 날짜형의 차이 값이 R1 ~ R2 사이일 경우
                    day['riskperiod'][i] = 1  # i번째 riskperiod 값에 1 지정
                elif (datetime.timedelta(days=C1) <= day.date[i] - day.VCNYMD[i] <= datetime.timedelta(
                        days=C2)):  # i번째 date-VCNYMD 날짜형의 차이 값이 C1 ~ C2 사이일 경우
                    day['riskperiod'][i] = 2  # i번째 riskperiod 값에 2 지정
        else:  # C3, C4에 입력 값이 모두 있을 경우 조건문
            for i in range(len(day)):  # day의 길이만큼 for 루프 진행
                if (datetime.timedelta(days=R1) <= day.date[i] - day.VCNYMD[i] <= datetime.timedelta(
                        days=R2)):  # i번째 date-VCNYMD 날짜형의 차이 값이 R1 ~ R2 사이일 경우
                    day['riskperiod'][i] = 1  # i번째 riskperiod 값에 1 지정
                elif (datetime.timedelta(days=C1) <= day.date[i] - day.VCNYMD[i] <= datetime.timedelta(
                        days=C2)):  # i번째 date-VCNYMD 날짜형의 차이 값이 C1 ~ C2 사이일 경우
                    day['riskperiod'][i] = 2  # i번째 riskperiod 값에 2 지정
                elif (datetime.timedelta(days=C3) <= day.date[i] - day.VCNYMD[i] <= datetime.timedelta(
                        days=C4)):  # i번째 date-VCNYMD 날짜형의 차이 값이 C3 ~ C4 사이일 경우
                    day['riskperiod'][i] = 2  # i번째 riskperiod 값에 2 지정

        # Riskperiod안에 질환 포함되는지 확인 outcome=1
        day['outcome'] = 0  # day에 outcome 컬럼 생성하며 0 값을 가짐
        for i in range(len(day)):  # day의 길이만큼 for 루프 진행
            if (day.date[i] == day.diag_dt[i]):  # i번째 행의 date컬럼값과 diag_dt값이 같을 경우
                day.outcome[i] = 1  # i번째 outcome값이 1을 가짐
        del (vac_diag5)

        ## SCRI 분석 (전체 대상자 분석)
        ## SCRI용 데이터셋 구축
        db_scri = day.copy()  # day1 를 db_scri 에 복사
        del (day)

        if len(db_scri) != 0:
            tmp_risk_1 = db_scri[db_scri.riskperiod == 1]
            tmp_risk_2 = db_scri[db_scri.riskperiod == 2]
            if (len(tmp_risk_1[tmp_risk_1.outcome == 1]) != 0) and (len(tmp_risk_2[tmp_risk_2.outcome == 1]) != 0):
                Result = scri_analysis(df=db_scri, sex_type=sex_type, Alz_date=date_range_strt,
                                       vac_count=vac_count)  # 선택한 성별에 맞는 데이터만을 가져옴

                final_result = final_result.append(Result)
                final_result.reset_index(drop=True, inplace=True)

    return final_result


def scri_analysis(df, sex_type, Alz_date, vac_count):
    df.reset_index(drop=True, inplace=True)
    # fu_stt에 date지정, fu_end에 date+1 지정
    scri = df[['RN_INDI', 'date', 'riskperiod', 'outcome']]  # df 에서 해당 컬럼들 데이터를 scri 에 지정
    scri['fu_stt'] = scri['date']  # scri의 fu_stt 컬럼 생성하며 date컬럼과 같은 값을 가짐
    scri['fu_end'] = scri['date'] + datetime.timedelta(days=1)  # scri의 fu_stt 컬럼 생성하며 date컬럼에 1일을 더한 값을 가짐
    scri = scri.sort_values(by=["RN_INDI", "date"])  # RN_INDI, date 순서로 오름차순으로 정렬

    # riskperiod/outcome/환자ID 를 비교하는 compare 변수 지정
    # scri에 compare 컬럼을 생성. 'outcome값-riskperiod값-RN_INDI값' 문자형 형식의 값을 가짐 (Ex. 1-1-101010)
    scri["compare"] = scri.outcome.astype(str) + "-" + scri.riskperiod.astype(str) + "-" + scri.RN_INDI.astype(str)
    scri["compare"] = scri["compare"].replace(" ", "")  # 위 코드에서 생성되는 문자 안의 띄어쓰기를 제거함

    # 첫 번째 환자이거나, compare값에 변화있을 경우 stt_flag = 1 지정 / 아닐 경우 결측치 지정
    # stt_flag 변화있을 경우 order +1
    scri2 = scri.copy()  # scri를 scri2에 복사함
    scri2["stt_flag"] = 0  # scri2에 stt_flag 컬럼을 생성
    scri2["order"] = 0  # scri2에 order 컬럼을 생성

    for i in range(len(scri2)):  # scri2 길이만큼 for 루프 진행
        if i == 0:  # 0번째 행(즉, 제일 첫 번째 행)의 경우
            scri2.stt_flag[i] = 1  # scri2에 stt_flag 값에 1을 가짐
            scri2.order[i] = 1  # scri2에 order 값에 1을 가짐
        elif scri2.RN_INDI[i] != scri2.RN_INDI[i - 1]:  # 위 조건이 아닌데, i번째 행의 RN_INDI 과 i-1번째 행의 RN_INDI 다른 경우
            scri2.stt_flag[i] = 1  # scri2에 stt_flag 값에 1을 가짐
            scri2.order[i] = scri2.order[i - 1] + 1  # i번째 order값이 i-1번째 order값에 1을 더한 값을 가짐
        elif scri2["compare"][i] != scri2["compare"][i - 1]:  # 위 조건이 아닌데, i번째 행의 compare 과 i-1번째 행의 compare 다른 경우
            scri2.stt_flag[i] = 1  # scri2에 stt_flag 값에 1을 가짐
            scri2.order[i] = scri2.order[i - 1] + 1  # i번째 order값이 i-1번째 order값에 1을 더한 값을 가짐
        elif scri2.stt_flag[i] == 0:  # 위 조건이 아닌데, i번째 행의 stt_flag값이 0 인 경우
            scri2.stt_flag[i] = "NA"  # scri2에 stt_flag 값에 "NA"을 가짐
            scri2.order[i] = scri2.order[i - 1]  # i번째 order값이 i번째이 i-1번재 order값을 가짐

    # 환자별 fu_stt 최소치, fu_end 최대치 기준으로 grouping
    # scri2에 outcome, RN_INDI, riskperiod, order 기준으로 group을 만든 후,
    # 각 그룹의 fu_stt에 각 그룹에서 fu_stt의 최소값, fu_end에 각 그룹에서 fu_end의 최대값을 가짐.
    # Ex. fu_stt = (1,2,3,4) fu_end = (1,2,3,4) 일 경우, 각 그룹을 한 행으로 묶은 후 fu_stt=1, fu_end=4 값을 가지게 됨
    scri3 = scri2.groupby([scri2.outcome, scri2.RN_INDI, scri2.riskperiod, scri2.order])
    scri4 = scri3.agg({'fu_stt': np.min, 'fu_end': np.max})
    scri4 = scri4.reset_index()

    # fu 날짜 interval를 fu_year 지정 및 ln 처리 값 l_fu_year 지정
    scri4["fu_year"] = 0.0  # scri4에 fu_year 컬럼을 생성하며 0.0 (float형태) 값을 가짐
    scri4["l_fu_year"] = 0.0  # scri4에 l_fu_year 컬럼을 생성하며 0.0 (float형태) 값을 가짐
    for i in range(len(scri4)):  # scri4 길이만큼 for 루프 진행
        scri4.fu_year[i] = float((scri4.fu_end[i] - scri4.fu_stt[
            i]).days) / 365.25  # i번째 fu_year값은 (i번째 fu_end - fu_stt)을 날짜로 환산한 후, float으로 형변환하여 365.25로 나눈 값을 가짐
        scri4.l_fu_year[i] = np.log(scri4.fu_year[i])  # i번재 l_fu_year값은 i번째 fu_year값의 ln() 자연로그 값을 가짐
    scri4 = scri4.sort_values(by=["RN_INDI", "fu_stt"])  # RN_INDI, fu_stt 순서로 오름차순으로 정렬
    scri4.reset_index(drop=True, inplace=True)

    ## Poisson 분포 기반 GEE regression 분석
    # Poisson 분포를 java에서 계산하는 방법이 있는지는 모르겠습니다.. 파이썬에서는 모듈을 불러와서 계산하면 되기 때문에 해당 모듈을 활용하여 계산하였습니다.


    scri4['riskperiod_0'] = 1 - scri4.riskperiod  # scri4에 riskperiod_0 컬럼을 생성하며 1 - riskperiod 값을 가짐
    scri4['riskperiod_1'] = scri4.riskperiod  # scri4에 riskperiod_0 컬럼을 생성하며 riskperiod 값을 가짐

    fam = sm.families.Poisson(sm.families.links.log())
    ind = sm.cov_struct.Independence()

    model = smf.gee(
        "outcome~riskperiod_1", "RN_INDI",
        data=scri4,
        offset="l_fu_year",
        cov_struct=ind,
        family=fam
    )
    result = model.fit()

    # GEE regression 분석 결과 정리
    temp1 = result.params  # temp1에 params (coefficient)값을 가짐
    temp2 = result.conf_int()  # temp2에 conf_int() 결과 값을 가짐
    data = {"Result_table": ['riskperiod_1'],  # 1번째 컬럼이름 Result_table의, 1번쨰 행은 riskperiod_0, 2번째 행은 riskperiod_1
            "estimate": [temp1[1]],  # 2번째 컬럼이름 estimate, 1번째 행 값은 0.0, 2번째 행 값은 temp1의 1번쨰 값 coefficient값을 가짐
            }
    Result_table = pd.DataFrame(data)  # 위 data형식의 Result_table의 데이터프레임 생성

    # 분석 결과 바탕으로 IRR 계산
    Result_table["irr"] = 0.0  # Result_table의 irr 컬럼을 생성하며 0.0 (float형태) 값을 가짐
    Result_table["irr"][0] = math.exp(Result_table.estimate[0])  # i번째 행의 irr값은 estimate값을 자연상수의 지수로 계산한 값을 가짐 (e^)
    Result_table['irr_cutoff'] = 1  # Result_table에 cutoff 컬럼 생성 및 값 1 생성

    # 구간 내 발생 환자 수 계산
    scri5 = scri4[scri4.outcome == 1]  # scri4에서 outcome 값이 1 인 데이터 scri5에 저장
    scri5.reset_index(drop=True, inplace=True)
    Result_table['calculated_time'] = Alz_date
    Result_table['injected_case'] = vac_count
    Result_table['risk_case'] = 0  # Result_table에 pat_num 컬럼 생성 및 값 0 생성
    Result_table['con_case'] = 0  # Result_table에 pat_num 컬럼 생성 및 값 0 생성
    Result_table.risk_case[0] = len(
        scri5[scri5.riskperiod == 1])  # Result_table의 pat_num의 1번째 행에 scri5에서 riskperiod 값이 1인 개수 지정
    Result_table.con_case[0] = len(
        scri5[scri5.riskperiod == 2])  # Result_table의 pat_num의 0번째 행에 scri5에서 riskperiod 값이 2인 개수 지정

    if sex_type != None:
        Result_table['sex'] = sex_type  # 1이면 남자, 2이면 여자

    return Result_table


def SCRI(research_dict, table_HOI, bfc_total, infrm_dth, vac):
    gender = research_dict.scri_gender
    vac_diag = data_integrating(research_dict=research_dict, table_HOI=table_HOI, bfc_total=bfc_total,
                                infrm_dth=infrm_dth, vac=vac)

    # SCRI 분석
    Result_df = scri_analysis_sex_type(research_dict=research_dict, df_vac_diag=vac_diag, sex_type=None,
                                       strt_date=int(research_dict.research_start_date),
                                       end_date=int(research_dict.research_end_date))
    # SCRI 성별 분석
    if (gender == 1):
        print('남성 분석')
        vac_diag = vac_diag[vac_diag.SEX == 1]
        Result_df_sex = scri_analysis_sex_type(research_dict=research_dict, df_vac_diag=vac_diag, sex_type=1,
                                           strt_date=int(research_dict.research_start_date),
                                           end_date=int(research_dict.research_end_date))
    elif (gender == 2):
        print('여성 분석')
        vac_diag = vac_diag[vac_diag.SEX == 2]
        Result_df_sex = scri_analysis_sex_type(research_dict=research_dict, df_vac_diag=vac_diag, sex_type=2,
                                           strt_date=int(research_dict.research_start_date),
                                           end_date=int(research_dict.research_end_date))
    elif (gender == 3):
        vac_sex_1 = vac_diag[vac_diag.SEX == 1]
        vac_sex_1.reset_index(drop=True, inplace=True)
        vac_sex_2 = vac_diag[vac_diag.SEX == 2]
        vac_sex_2.reset_index(drop=True, inplace=True)
        print('남성 분석')
        Result_df_sex = scri_analysis_sex_type(research_dict=research_dict, df_vac_diag=vac_sex_1, sex_type=1,
                                           strt_date=int(research_dict.research_start_date),
                                           end_date=int(research_dict.research_end_date))
        print('여성 분석')
        Result_df_sex = Result_df_sex.append(
            scri_analysis_sex_type(research_dict=research_dict, df_vac_diag=vac_sex_2, sex_type=2,
                                   strt_date=int(research_dict.research_start_date),
                                   end_date=int(research_dict.research_end_date)))
        Result_df_sex.reset_index(drop=True, inplace=True)
    else:
        Result_df_sex = pd.DataFrame()

    ## 아래 DB 컬럼들 추가 ##
    vaccine = research_dict.vaccine_target_id
    vcntme = research_dict.vcntime
    hoidefn = research_dict.hoidefn
    studydesign = research_dict.studydesignid

    if len(Result_df) == 0:
        print("No variable matching")
        result_table = pd.DataFrame()
    else:
        results = ({
            'studydesign': studydesign,
            'vaccine': vaccine,
            'vcntime': vcntme,
            'hoidefn': hoidefn,
            'calculated_date': Result_df.calculated_time + '-28',
            'irr': Result_df.irr,
            'irr_cutoff': Result_df.irr_cutoff,
            'injected_case': Result_df.injected_case,
            'risk_case': Result_df.risk_case,
            'con_case': Result_df.con_case,
        })
        result_table = pd.DataFrame.from_dict(results)

    if len(Result_df_sex) == 0:
        print("No variable matching for gender table")
        result_table_sex = pd.DataFrame()
    else:
        results_sex = ({
            'studydesign': studydesign,
            'vaccine': vaccine,
            'vcntime': vcntme,
            'hoidefn': hoidefn,
            'calculated_date': Result_df_sex.calculated_time + '-28',
            'irr': Result_df_sex.irr,
            'irr_cutoff': Result_df_sex.irr_cutoff,
            'injected_case': Result_df_sex.injected_case,
            'risk_case': Result_df_sex.risk_case,
            'con_case': Result_df_sex.con_case,
            'sex': Result_df_sex.sex
        })
        result_table_sex = pd.DataFrame.from_dict(results_sex)

    return result_table, result_table_sex

#
# if __name__ == '__main__':
#     table_HOI, vac, bfc, _, _, _, _, death = sc.connect_db(research_dict)
#     print('DB reading is done.')
#     results = SCRI(research_dict, table_HOI, bfc, death, vac)
#     print(results)
