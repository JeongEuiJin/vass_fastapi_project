##main.py
import sys
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import datetime
import re
from datetime import datetime
from datetime import datetime, timedelta
import multiprocessing as mp
from functools import partial
from itertools import chain
import random 
#아래 4개의 모듈은 머신러닝 모듈입니다
from sklearn.ensemble import *
from sklearn.metrics import *
from sklearn.model_selection import *
from sklearn.model_selection import RandomizedSearchCV

#함수화 한 놓은 코드 import
import duration as d
import join as 
import extract as e
import ML_analysis as ml


source_data_dir= #데이터가 들어있는 폴더 (질병관리청에 이관했을 경우 질병관리청 데이터가 들어있는 폴더 디렉토리를 지정할 예정입니다. 현재는 전달드린 t20, t30 파일 등이 들어있는 폴더를 지정하시면 됩니다.)
data_save_dir= # 분석 결과는 서버에 저장될 것이므로, 이 부분은 분석 결과 서버에 결과를 넣도록 설정해 주시면 됩니다. AnalysisResultsML table
atc_n='4' #고정


##READ DATA 
## 아래 4줄에 해당하는 코드는 미작성 상태입니다. 편하신 방법으로 작성해 주시면 감사하겠습니다. (SCRI 코드와 동일함)
# 분석 대상 이상반응의 조작적 정의 선택 부분에서 가져온 HOI 내 생성된 쿼리를 작동시켜,
# 해당 HOI를 만족하는 환자 조건을 추출하는 코드를 작성해 주세요.
# 해당 테이블을 table_HOI라고 지칭하겠습니다.
# table_HOI 내에'MDCARE_STRT_DT' 가 [연구디자인 생성 - 분석 대상 데이터 기간] 에 포함되는 것들만 떼어서 table_HOI에 저장합니다.

# table_HOI에 있는 환자ID (indi_Dscm_no)에 해당하는 처방 기록 추출
# 이 때, 각 환자 별로 [연구디자인 생성 - 분석 대상 데이터 기간] 에 해당하는 처방 기록만을 추출 (t30과 t60에서)
HOI_patients = table_HOI.INDI_DSCM_NO.tolist()
table30=pd.read_csv(os.path.join(source_data_dir,'t30.csv'))
table30=table30[['CMN_KEY','MDCARE_STRT_DT','MCARE_DIV_CD']] 
table30=table30.query("MDCARE_STRT_DT<분석 대상 데이터 기간의 뒤 날짜 & 분석 대상 데이터 기간의 앞 날짜<=MDCARE_STRT_DT")
table30.rename(columns={'MDCARE_STRT_DT':'drug_date'}, inplace=True)
table30=table30.merge(table_HOI, how='inner', on='CMN_KEY')
table30=table30.INDI_DSCM_NO.isin(HOI_patients)
table30=table30.rename(columns={'MCARE_DIV_CD_ADJ':'GNL_NM_CD'}, inplace=True)


table60=pd.read_csv(os.path.join(source_data_dir,'t60.csv')) 
table60=table60[['CMN_KEY','MDCARE_STRT_DT','GNL_NM_CD']] 
table60=table60.query("MDCARE_STRT_DT<분석 대상 데이터 기간의 뒤 날짜 & 분석 대상 데이터 기간의 앞 날짜<=MDCARE_STRT_DT")
table60.rename(columns={'MDCARE_STRT_DT':'drug_date'}, inplace=True)
table60=table60.merge(table_HOI, how='inner', on='CMN_KEY')
table60=table60.INDI_DSCM_NO.isin(HOI_patients)

target_total_pre=pd.concat([table30, table60]) #table30, 60 전처리 완료된 파일 합치기

GNL2ATC=pd.read_csv(os.path.join('GNL2ATC_complete가 있는 폴더 경로','GNL2ATC_complete.csv'),index_col=0) #고정
GNL2ATC['ATC_N']=GNL2ATC['ATC'].str[:atc_n] #ATC코드라는 컬럼이 총 6-8자리로 이루어진 컬럼인데 앞의 4자리만 활용하기 위해 따로 떼어서 ATC_N 컬럼으로 분리함

vac=pd.read_csv(os.path.join(source_data_dir,'vac.csv')) 
bfc = pd.read_csv(os.path.join(source_data_dir,'bfc.csv')) 


## CASE / CONTROL DURATION 
# gap_delta : [연구디자인 생성 - 분석 대상 이상반응의 조작적 정의 선택 - gap era]
target_diag_duration = d.case_groupby(table20, gap_delta)

# 아래에서 window_size는 [연구디자인 생성 - machine learning - 위험 및 대조구간 길이] 입니다
target_date = d.CaseDuration(target_diag_duration, window_size = 14).target_case_duration()

# 아래에서 window_size는 [연구디자인 생성 - machine learning - 위험 및 대조구간 길이] 입니다
# step_day 는 7로 고정입니다.
targt_cont_duration = d.ControlDuration(target_diag_duration, window_size= 14, step_day= 7).target_cont_duration()

target_total = pd.concat([target_date,targt_cont_duration ]) #위에서 만든 두 개의 테이블을 합침


## 각 구간 동안의 처방 데이터 추출
drug_output = e.Extract_ml_input(target_total_pre, target_total, GNL2ATC)    

##만들어진 데이터셋에 bfc, vac 테이블응ㄹ Join 합니다.
input_df = j.Join(target_total, vac, drug_output, bfc).BFC()

# # extract window 
input_data=e.extract_window(input_df,window_size= 14) 

# # Run machine learning
result= m.ML_run(input_data).ML()
