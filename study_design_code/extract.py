##extract.py
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import datetime
import re
from datetime import timedelta, datetime
from datetime import datetime
from datetime import datetime, timedelta
import multiprocessing as mp
from functools import partial
from itertools import chain
import random 

#처방 데이터 추출 (main.py의 read data 부분에서 만든 target_total_pre와 case/control에서 만든 target_total을 활용합니다.)
def Extract_ml_input(target_total_pre, target_total, GNL2ATC): 
    input_data_list=list()
    target_total_pre.rename(columns={'start_day'='MDCARE_STRT_DT'}, inplace=True)
    for idx,(key1,group) in enumerate(target_total_pre.groupby('INDI_DSCM_NO')):   
        group['MDCARE_STRT_DT'] = group['MDCARE_STRT_DT'].apply(lambda x : datetime.strptime(str(x),'%Y%m%d'))
        target_total["start_day"]= target_total["start_day"].astype("datetime64") 
        target_total["end_day"]=  target_total["end_day"].astype("datetime64")  
        patient_drug = pd.merge(target_total,group,on='INDI_DSCM_NO').query("MDCARE_STRT_DT<end_day & start_day<=MDCARE_STRT_DT")
        
        for key2,case_group in patient_drug.groupby(['start_day','end_day']):
            info = [key1,key2[0],key2[1]]
            case_df = pd.DataFrame(info).T
            case_df.columns = ['INDI_DSCM_NO','start_day','end_day']

            drug_df = pd.DataFrame(case_group.GNL_NM_CD.value_counts()).reset_index()
            end_day =datetime.strptime( str(key2[1])[:10],'%Y-%m-%d')
      
            drug_df = pd.merge(GNL2ATC,drug_df,left_on='GNL',right_on='index',how='inner')['ATC_N'].value_counts()
            drug_df=pd.DataFrame(drug_df).T.reset_index(drop=True)

            case_df = pd.concat([case_df,drug_df],axis=1)
            if drug_df.sum(axis=1).values[0] !=0:
                input_data_list.append(case_df.copy())

    if len(input_data_list)==0:
        print(key1)
        return None
    else : 
        return pd.concat(input_data_list)


# machine learning을 위한 최종 데이터 추출. 및 Outlier 처리
# window_size : [연구디자인 생성 - Machine learning - 위험구간 및 대조구간 길이]
def extract_window(input_df,window_size:int):
    tmp = input_df.drop(['case','end_day','INDI_DSCM_NO','start_day'],axis=1)
    input_cols = tmp.columns.tolist()
    save_cols = ['case','end_day','INDI_DSCM_NO','start_day','STD_YYYY', 'SEX_TYPE', 'BYEAR']
    atc_cols = list(set(input_cols)-set(save_cols))
    
    taget_atc =  tmp[atc_cols]
    taget_atc[save_cols] = input_df[save_cols]
    
    
    ## atc_col의 합이 0인 기록 제거하기
    sum_df = target_atc[atc_cols].sum(axis=1)
    sum_df = sum_df[sum_df==0]
    sum_zero_idx = sum_df.index
        
    target_atc.drop(sum_zero_idx,inplace=True)
    
    ## END,STR DAY 지우기 
    target_atc.drop(['start_day','end_day'],axis=1,inplace=True)

    # 나이 성별 변수 추가
    input_age = [  int(VY)-int(birth)+1   for birth, VY in zip(target_atc.BYEAR,target_atc.STD_YYYY)]
    target_atc['age'] = input_age
    target_atc.drop(STD_YYYY, inplace=True)
    target_atc['SEX_TYPE']=(target_atc['SEX_TYPE']-2)*(-1)
    
    # 머신러닝 데이터셋 생성 완료
    # 환자ID, 약물 코드, 나이, 성별, 백신접종 여부, 위험/대조구간 여부(case)만 포함되어 있음
    return target_atc

