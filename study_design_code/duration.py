#duration.py
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import datetime
import re
from datetime import timedelta, datetime

  ##table_HOI에서 INDI_DSCM_NO (환자) 별로, 시작일 (가장 빠른 날짜), 마지막일 (가장 늦은 날짜) 3개 컬럼 생성 
def case_groupby(table_HOI, gap_delta):
    total_n = len(table_HOI.INDI_DSCM_NO.unique())
    table_HOI['MDCARE_DATETIME'] = table_HOI.MDCARE_STRT_DT.apply(lambda x :datetime.strptime(str(x),'%Y%m%d')) #형변환
    total_duration_df=[]
    for n,(idx,group) in enumerate(table_HOI.groupby('INDI_DSCM_NO')): #각 환자ID 별로 그룹 짓기
        row_numbers = list() #같은 HOI구간을 나타내기 위한 row-number list
        group = group.sort_values('MDCARE_DATETIME')
        group['diff_day'] = group['MDCARE_DATETIME'] - group['MDCARE_DATETIME'].shift(1)
        tmp_diff = group.diff_day.values.astype(int)
        tmp_diff[0]=0
        str_n = 1
        gap_delta = # [연구디자인 생성 - 분석 대상 이상반응의 조작적 정의 선택 - gap era]
        for day in tmp_diff:
          if day<gap_delta:
              row_numbers.append(str_n)
          elif day>=gap_delta:
              str_n+=1
              row_numbers.append(str_n)
        group['row_number'] = row_numbers
        str_dates = group.groupby('row_number').head(1).MDCARE_DATETIME.values
        end_datset = group.groupby('row_number').tail(1).MDCARE_DATETIME.values
        duration_df = pd.DataFrame(np.c_[str_dates,end_datset],columns=['start_day','end_day'])
        duration_df['INDI_DSCM_NO'] = idx
        total_duration_df.append(duration_df)
    target_diag_duration = pd.concat(total_duration_df)
    return target_diag_duration #target_diag_duration = table_HOI로부터 INDI_DSCM_NO (환자) 별로, 시작일 (가장 빠른 날짜), 마지막일 (가장 늦은 날짜) 3개 컬럼 생성 
 

# 아래에서 window_size는 [연구디자인 생성 - machine learning - 위험 및 대조구간 길이] 입니다.
#환자ID 별로 위험 구간을 추출하는 코드입니다. 위 target_diag_duration 테이블에서 시작일 이전 14일~시작일 만큼을 위험 구간으로 만듭니다.
class CaseDuration: 
    def __init__(self,groupby, window_size):
        self.window_size= window_size
        self.group=groupby

    def target_case(self): 
        group_list=list()
        for key,group in self.group.sort_values(['INDI_DSCM_NO','start_day']).groupby('INDI_DSCM_NO'):
            if len(group)>1:
                group = group.sort_values('start_day')
                group['start_day'] = group['start_day'].astype(str)
                group['end_day'] = group['end_day'].astype(str) 
                end_day = group['end_day'].apply(lambda x : datetime.strptime(x,'%Y-%m-%d') )
                str_day = group['start_day'].apply(lambda x : datetime.strptime(x,'%Y-%m-%d') )
                group_list.append(group)

            else:
                group_list.append(group)
            
        target_case = pd.concat(group_list)
        return target_case
  
    def target_case_duration(self):
        z=self.target_case()
    
        end_day =z.start_day.apply(lambda x:datetime.strptime(str(x)[:10],"%Y-%m-%d")-timedelta(days=0))
        str_day = z.start_day.apply(lambda x:datetime.strptime(str(x)[:10],"%Y-%m-%d")-timedelta(days=self.window_size))

        end_day=end_day.apply(lambda x: datetime.strftime(x,"%Y-%m-%d"))
        str_day=str_day.apply(lambda x: datetime.strftime(x,"%Y-%m-%d"))

        target_date=pd.DataFrame({'start_day':str_day, "end_day":end_day}).reset_index(drop=True,inplace=False)
        target_date['INDI_DSCM_NO'] = z['INDI_DSCM_NO'].tolist()
        target_date['case']=1

        return target_date

#환자ID 별로 대조 구간을 추출하는 코드입니다. 위 코드를 통해 생성된 위험 구간을 제외한 구간 중 추출합니다.
class ControlDuration:
  def __init__(self, target_diag_duration,table40, window_size,step_day):
    self.target_diag_duration = target_diag_duration
    self.table40= table40
    self.window_size= window_size
    self.step_day=step_day

  def cont_avail_df(self):
     #target_date에서 추출로 뽑힌 기간 제외하고 Control 구간 뽑기
      # 컨트롤 추출 기간 설정하는 WINDOW SIZE 일 기간 설정 
    cont_avail_list = list()
    for idx,(key,group) in enumerate(self.target_diag_duration.groupby('INDI_DSCM_NO')):
        group['start_day'] = group['start_day'].astype(str)
        group['end_day'] = group['end_day'].astype(str)   

        if group['start_day']<'관찰 기관의 앞 날짜':

          #만약 한 환자에 있어 start_day가 [연구디자인 생성 - 관찰 기간]의 시작 일자 이후 6개월 이내라면 위험 구간의 end_day의 1주 뒤부터 관찰 기간의 마지막 날짜 사이에서 대조구간을 추출함
          str_day = datetime.strptime(group['start_day'].values[0],'%Y-%m-%d')
          cont_str = datetime.strftime(end_day + timedelta(days=7),'%Y-%m-%d') #cont_str = end_day일 1주 후 
          cont_end = datetime.strftime('관찰 기간의 뒷 날짜' ,'%Y-%m-%d') #cont_end = 관찰 기간 뒷 날짜
          cont_avail_list.append([cont_str,cont_end,key])

          # 사이에서 뽑기
          if len(group)>1:
            end_day = group['end_day'].apply(lambda x : datetime.strptime(x,'%Y-%m-%d') ) +  timedelta(days=365)
            str_day = group['start_day'].apply(lambda x : datetime.strptime(x,'%Y-%m-%d') ) - timedelta(days=7*4)
            tmp = [[str_time[:10],end_time[:10],pid] for str_time,end_time,pid in zip(str_day.values[1:].astype(str),end_day.values[:-1].astype(str),[key]*len(end_day.values[:-1]))]
            cont_avail_list.extend(tmp)

        else:
          #만약 한 환자에 있어 start_day가 [연구디자인 생성 - 관찰 기간]의 시작 일자 6개월 후 라면 관찰 기간의 시작 날짜와 위험 구간 시작 날짜 1주 전 사이에서 대조구간을 추출함
          str_day = datetime.strptime(group['start_day'].values[0],'%Y-%m-%d')
          cont_end = datetime.strftime(str_day - timedelta(days=7),'%Y-%m-%d') #cont_str = str_day일 1주 전 
          cont_str = datetime.strftime('관찰 기간의 시작 날짜' ,'%Y-%m-%d') #cont_str = 관찰 기간 앞 날짜
          cont_avail_list.append([cont_str,cont_end,key])

          # 사이에서 뽑기
          if len(group)>1:
            end_day = group['end_day'].apply(lambda x : datetime.strptime(x,'%Y-%m-%d') ) +  timedelta(days=365)
            str_day = group['start_day'].apply(lambda x : datetime.strptime(x,'%Y-%m-%d') ) - timedelta(days=7*4)
            tmp = [[str_time[:10],end_time[:10],pid] for str_time,end_time,pid in zip(str_day.values[1:].astype(str),end_day.values[:-1].astype(str),[key]*len(end_day.values[:-1]))]
            cont_avail_list.extend(tmp)

    cont_avail_df = pd.DataFrame(cont_avail_list,columns=['start_day','end_day','INDI_DSCM_NO'])
    return cont_avail_df


  def target_cont_duration(self): 
      # 위에서 만들어진 cont_avail_df에서 start_day와 end_day 사이에서 7일 간격으로 추출하여 1인당 2개의 대조 구간만을 추출하는 코드 
      y=self.cont_avail_df()
      cont_day_list = list()
   
      for i,row in y.iterrows():
          str_day = datetime.strptime(row.start_day,'%Y-%m-%d')
          end_day = datetime.strptime(row.end_day,'%Y-%m-%d')
          diff_day = (end_day-str_day).days
          pid=row.INDI_DSCM_NO

          day_steps = [str_day + timedelta(days=step) for step in range(0,diff_day-self.step_day,self.step_day)]
          day_steps = [[pid,datetime.strftime(day,'%Y-%m-%d'),datetime.strftime(day+timedelta(days=self.window_size),'%Y-%m-%d')] for day in day_steps]
          cont_day_list.extend(day_steps)
      target_cont_duration = pd.DataFrame(cont_day_list,columns=['INDI_DSCM_NO','start_day','end_day']) 
      target_cont_duration.groupby('INDI_DSCM_NO').apply(lambda x: x.sample(2)).reset_index(drop=True)
      target_cont_duration['case']=0
      return target_cont_duration
