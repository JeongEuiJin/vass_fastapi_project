#join.py
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import datetime
import re
from datetime import timedelta, datetime

import multiprocessing as mp
from functools import partial
from itertools import chain
import random 


# 백신, 약물, 인적 정보 합치기 
class Join: 
    def __init__(self,target_total, vaccine, drug_output, BFC):
        self.target_total=target_total
        self.vaccine =vaccine
        self.drug_output =drug_output #약물 정보 
        self.bfc= BFC

    def Vac(self):
    #연구 대상자 id 추출
        target_pid = pd.DataFrame(self.target_total.INDI_DSCM_NO.unique(),columns=['INDI_DSCM_NO']) 
        # 연구대상자 중 백신 맞은 사람들 정보 (id, vac 맞은 날짜, 백신 여부 )
        vaccine_in_target = pd.merge(self.vaccine,target_pid,how='inner',on='INDI_DSCM_NO',copy=False)
        # target case/con에 백신 접종 정보 삽입
        target_vaccine_merge = pd.merge(self.target_total,vaccine_in_target,on='INDI_DSCM_NO',how='left',copy=False)
        target_vaccine_merge=target_vaccine_merge.astype({"VCNYMD":'datetime64',"start_day":'datetime64',"end_day":'datetime64' })
        #추가 -unnamed 열 제거 
        target_vaccine_merge=target_vaccine_merge.drop("Unnamed: 0", axis=1)
        target_in_vaccine = target_vaccine_merge.query("(VCNYMD)>=start_day & (VCNYMD)<end_day",)
        # 추가 -중복 제거 
        target_in_vaccine = target_in_vaccine.drop_duplicates()

        tmp = target_in_vaccine.apply(lambda x : (x.end_day - x.VCNYMD).days,axis=1)
        target_in_vaccine['VCN_diff']=tmp
        target_in_vaccine['VCN'] =1

        target_in_vaccine2 = target_in_vaccine[['INDI_DSCM_NO','VCN_diff','VCN','end_day','start_day']]

        # 추가, merge하기 위해선 둘의 데이터 타입이 동일해야 하므로 타입 일치 
        self.target_total= self.target_total.astype({"start_day":'datetime64',"end_day":'datetime64' })

        #  target_total에 반영 
        final_target_total = pd.merge(self.target_total,target_in_vaccine2,on=['INDI_DSCM_NO','end_day','start_day'],how='left')
        final_target_total = final_target_total.fillna(0)
        final_target_total.reset_index(drop=True,  inplace=True)
        self.drug_output.reset_index(drop=True, inplace=True)
        self.drug_output.rename({'pid':'INDI_DSCM_NO'}, axis=1, inplace=True)
        drug_output_vac = pd.merge(self.drug_output,final_target_total[['INDI_DSCM_NO','end_day','start_day','VCN_diff','VCN',"case"]],how='outer',on=['INDI_DSCM_NO','end_day','start_day'])
        drug_output_vac.fillna(0,inplace=True)
          # 추가 -중복 제거 
        drug_output_vac = drug_output_vac.drop_duplicates()
        return  drug_output_vac

    def VacDrug(self):
        drug_output_vac=self.Vac()   
        info_cols = ['end_day','INDI_DSCM_NO','start_day','case']

        target_cols = set(drug_output_vac.columns) - set(info_cols)
        target_cols = list(target_cols)
        target_cols = sorted(target_cols)

        info_cols_df = drug_output_vac[info_cols]

        drug_output_vac.drop(info_cols,axis=1,inplace=True)
        flag = drug_output_vac.sum(axis=1)!=0
        drug_output_vac.drop(flag[~flag].index,inplace=True) 
        info_cols_df.drop(flag[~flag].index,inplace=True)

        for colname in info_cols:
            drug_output_vac[colname] = info_cols_df[colname]
        # 추가 -중복 제거 
        drug_output_vac = drug_output_vac.drop_duplicates()

        return drug_output_vac

    def BFC(self):
        drug_output_vac=self.VacDrug()  
        drug_output_vac['STD_YYYY'] = drug_output_vac.start_day.apply(lambda x: int(str(x)[:4]))
        input_final_bfc = pd.merge(drug_output_vac,self.bfc, on=['INDI_DSCM_NO','STD_YYYY'],how='inner',copy=False)
            # 추가 -중복 제거 
        input_final_bfc = input_final_bfc.drop_duplicates()
        return input_final_bfc


