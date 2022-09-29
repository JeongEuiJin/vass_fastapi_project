# join.py
import pandas as pd


# 백신, 약물, 인적 정보 합치기 
class Join:
    def __init__(self, target_total, vaccine, drug_output, BFC, age_type):
        self.target_total = target_total
        self.vaccine = vaccine
        self.drug_output = drug_output  # 약물 정보
        self.bfc = BFC
        self.age_type = age_type

    def Vac(self):
        # 연구 대상자 id 추출
        target_pid = pd.DataFrame(self.target_total.RN_INDI.unique(), columns=['RN_INDI'])
        # 연구대상자 중 백신 맞은 사람들 정보 (id, vac 맞은 날짜, 백신 여부 )
        target_pid['RN_INDI']=target_pid['RN_INDI'].astype(int)
        self.vaccine['RN_INDI']=self.vaccine['RN_INDI'].astype(int)
        vaccine_in_target = pd.merge(self.vaccine, target_pid, how='inner', on='RN_INDI', copy=False)
        # target case/con에 백신 접종 정보 삽입
        self.target_total['RN_INDI']=self.target_total['RN_INDI'].astype(int)
        target_vaccine_merge = pd.merge(self.target_total, vaccine_in_target, on='RN_INDI', how='left', copy=False)
        target_vaccine_merge = target_vaccine_merge.astype(
            {"VCNYMD": 'datetime64', "start_day": 'datetime64', "end_day": 'datetime64'})
        target_in_vaccine = target_vaccine_merge.query("(VCNYMD)>=start_day & (VCNYMD)<end_day", )
        target_in_vaccine = target_in_vaccine.drop_duplicates()

        tmp = target_in_vaccine.apply(lambda x: (x.end_day - x.VCNYMD).days, axis=1)
        target_in_vaccine['VCN'] = 1

        target_in_vaccine2 = target_in_vaccine[['RN_INDI', 'VCN', 'end_day', 'start_day']]

        # 추가, merge하기 위해선 둘의 데이터 타입이 동일해야 하므로 타입 일치 
        self.target_total = self.target_total.astype({"start_day": 'datetime64', "end_day": 'datetime64'})
        self.drug_output = self.drug_output.astype({"start_day": 'datetime64', "end_day": 'datetime64'})

        #  target_total에 반영 
        final_target_total = pd.merge(self.target_total, target_in_vaccine2, on=['RN_INDI', 'end_day', 'start_day'],
                                      how='left')
        final_target_total = final_target_total.fillna(0)
        final_target_total.reset_index(drop=True, inplace=True)
        self.drug_output.reset_index(drop=True, inplace=True)
        self.drug_output['RN_INDI']=self.drug_output['RN_INDI'].astype(int)
        drug_output_vac = pd.merge(self.drug_output,
                                   final_target_total[['RN_INDI', 'end_day', 'start_day', 'VCN', "case"]], how='outer',
                                   on=['RN_INDI', 'end_day', 'start_day'])
        drug_output_vac.fillna(0, inplace=True)
        # 추가 -중복 제거
        drug_output_vac = drug_output_vac.drop_duplicates()
        return drug_output_vac

    def VacDrug(self):
        drug_output_vac = self.Vac()
        info_cols = ['end_day', 'RN_INDI', 'start_day', 'case']

        target_cols = set(drug_output_vac.columns) - set(info_cols)
        target_cols = list(target_cols)
        target_cols = sorted(target_cols)

        info_cols_df = drug_output_vac[info_cols]

        drug_output_vac.drop(info_cols, axis=1, inplace=True)
        flag = drug_output_vac.sum(axis=1) != 0
        drug_output_vac.drop(flag[~flag].index, inplace=True)
        info_cols_df.drop(flag[~flag].index, inplace=True)

        for colname in info_cols:
            drug_output_vac[colname] = info_cols_df[colname]
        # 추가 -중복 제거 
        drug_output_vac = drug_output_vac.drop_duplicates()

        return drug_output_vac

    def BFC(self):
        drug_output_vac = self.VacDrug()
        drug_output_vac['STD_YYYY'] = drug_output_vac.start_day.apply(lambda x: int(str(x)[:4]))
        self.bfc['RN_INDI'] = self.bfc['RN_INDI'].astype(int)
        input_final_bfc = pd.merge(drug_output_vac, self.bfc[['RN_INDI', 'STD_YYYY', 'SEX', self.age_type]],
                                   on=['RN_INDI', 'STD_YYYY'], how='inner', copy=False)
        # 추가 -중복 제거
        input_final_bfc = input_final_bfc.drop_duplicates()
        return input_final_bfc
