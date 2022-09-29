# duration.py
from datetime import timedelta, datetime

import numpy as np
import pandas as pd


##table_HOI에서 RN_INDI (환자) 별로, 시작일 (가장 빠른 날짜), 마지막일 (가장 늦은 날짜) 3개 컬럼 생성
def case_groupby(table_HOI, gap_delta):
    total_n = len(table_HOI.RN_INDI.unique())
    table_HOI['MDCARE_DATETIME'] = table_HOI.MDCARE_STRT_DT.apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))  # 형변환
    total_duration_df = []
    for n, (idx, group) in enumerate(table_HOI.groupby('RN_INDI')):  # 각 환자ID 별로 그룹 짓기
        row_numbers = list()  # 같은 HOI구간을 나타내기 위한 row-number list
        group = group.sort_values('MDCARE_DATETIME')
        group['diff_day'] = group['MDCARE_DATETIME'] - group['MDCARE_DATETIME'].shift(1)
        tmp_diff = group.diff_day.dt.days
        tmp_diff.fillna(0, inplace=True)
        str_n = 1

        for day in tmp_diff:
            if day < gap_delta:
                row_numbers.append(str_n)
            elif day >= gap_delta:
                str_n += 1
                row_numbers.append(str_n)
        group['row_number'] = row_numbers
        str_dates = group.groupby('row_number').head(1).MDCARE_DATETIME.values
        end_datset = group.groupby('row_number').tail(1).MDCARE_DATETIME.values
        duration_df = pd.DataFrame(np.c_[str_dates, end_datset], columns=['start_day', 'end_day'])
        duration_df['RN_INDI'] = idx
        total_duration_df.append(duration_df)
    target_diag_duration = pd.concat(total_duration_df)
    return target_diag_duration  # target_diag_duration = table_HOI로부터 INDI_DSCM_NO (환자) 별로, 시작일 (가장 빠른 날짜), 마지막일 (가장 늦은 날짜) 3개 컬럼 생성


# 아래에서 window_size는 [연구디자인 생성 - machine learning - 위험 및 대조구간 길이] 입니다.
# 환자ID 별로 위험 구간을 추출하는 코드입니다.
class CaseDuration:
    def __init__(self, groupby, window_size):
        self.window_size = window_size
        self.group = groupby

    def target_case(self):
        group_list = list()
        for key, group in self.group.sort_values(['RN_INDI', 'start_day']).groupby('RN_INDI'):
            if len(group) > 1:
                group = group.sort_values('start_day')
                group['start_day'] = group['start_day'].astype(str)
                group['end_day'] = group['end_day'].astype(str)

                end_day = group['end_day'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
                str_day = group['start_day'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))

                diff_days = (str_day.values[1:] - end_day.values[:-1]).astype('timedelta64[D]').astype(int)
                diff_flag = list(diff_days > 180)
                diff_flag = [True] + diff_flag

                group = group[diff_flag]
                group_list.append(group)

            else:
                group_list.append(group)

        target_case = pd.concat(group_list)
        return target_case

    def target_case_duration(self):
        z = self.target_case()

        end_day = z.start_day.apply(lambda x: datetime.strptime(str(x)[:10], "%Y-%m-%d") - timedelta(days=0))
        str_day = z.start_day.apply(
            lambda x: datetime.strptime(str(x)[:10], "%Y-%m-%d") - timedelta(days=self.window_size))

        end_day = end_day.apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
        str_day = str_day.apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))

        target_date = pd.DataFrame({'start_day': str_day, "end_day": end_day}).reset_index(drop=True, inplace=False)
        target_date['RN_INDI'] = z['RN_INDI'].tolist()
        target_date['case'] = 1

        return target_date


# 환자ID 별로 대조 구간을 추출하는 코드입니다. 위 코드를 통해 생성된 위험 구간을 제외한 구간 중 추출합니다.
class ControlDuration:
    def __init__(self, target_diag_duration, research_end_date, window_size, step_day):
        self.target_diag_duration = target_diag_duration
        self.window_size = window_size
        self.step_day = step_day
        self.research_end_date = research_end_date

    def cont_avail_df(self):

        # HOI(T20) 에서 추출로 뽑힌 기간 제외하고 Control 구간 뽑기
        # 컨트롤 추출 기간 설정하는 WINDOW SIZE 일 기간 설정
        cont_avail_list = list()
        for idx, (key, group) in enumerate(self.target_diag_duration.groupby('RN_INDI')):
            # cont_end = hoi 처음 발생일 4주전
            # cont_str = cont구간은 총 6개월
            group['start_day'] = group['start_day'].astype(str)
            group['end_day'] = group['end_day'].astype(str)
            # 제일 앞부분 cont 뽑는 기간 찾는 부분
            str_day = datetime.strptime(group['start_day'].values[0], '%Y-%m-%d')
            cont_end = datetime.strftime(str_day - timedelta(days=7 * 4), '%Y-%m-%d')
            cont_str = datetime.strftime(str_day - timedelta(days=7 * 4 + 7 * 4 * 6), '%Y-%m-%d')
            cont_avail_list.append([cont_str, cont_end, key])

            # 제일 끝에서 1년뒤
            end_day = datetime.strptime(group['end_day'].values[-1], '%Y-%m-%d')
            cont_str = datetime.strftime(end_day + timedelta(days=365), '%Y-%m-%d')
            cont_str = datetime.strptime(cont_str, '%Y-%m-%d').date()
            research_end_date = pd.to_datetime(str(self.research_end_date), format='%Y-%m-%d').date()

            if cont_str < research_end_date:
                cont_avail_list.append([cont_str, research_end_date, key])

            # 사이에서 뽑기
            if len(group) > 1:
                end_day = group['end_day'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d')) + timedelta(days=365)
                str_day = group['start_day'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d')) - timedelta(days=7 * 4)
                tmp = [[str_time[:10], end_time[:10], pid] for str_time, end_time, pid in
                       zip(str_day.values[1:].astype(str), end_day.values[:-1].astype(str),
                           [key] * len(end_day.values[:-1]))]
                cont_avail_list.extend(tmp)

        cont_avail_df = pd.DataFrame(cont_avail_list, columns=['start_day', 'end_day', 'RN_INDI'])
        return cont_avail_df

    def target_cont_duration(self):
        # 위에서 만들어진 cont_avail_df에서 start_day와 end_day 사이에서 7일 간격으로 추출하여 1인당 2개의 대조 구간만을 추출하는 코드

        # 위에서 만들어진 cont_avail_df에서 start_day와 end_day 사이에서 7일 간격으로 추출하여 1인당 2개의 대조 구간만을 추출하는 코드
        y = self.cont_avail_df()
        cont_day_list = list()

        for i, row in y.iterrows():
            str_day = datetime.strptime(str(row.start_day), '%Y-%m-%d')
            end_day = datetime.strptime(str(row.end_day), '%Y-%m-%d')
            diff_day = (end_day - str_day).days
            pid = row.RN_INDI

            day_steps = [str_day + timedelta(days=step) for step in range(0, diff_day - 7, 7)]
            day_steps = [
                [pid, datetime.strftime(day, '%Y-%m-%d'), datetime.strftime(day + timedelta(days=14), '%Y-%m-%d')] for
                day in day_steps]
            cont_day_list.extend(day_steps)
        target_cont_duration = pd.DataFrame(cont_day_list, columns=['RN_INDI', 'start_day', 'end_day'])

        target_cont_duration['case'] = 0
        target_cont_duration['start_day'] = target_cont_duration['start_day'].apply(
            lambda x: datetime.strptime(str(x), '%Y-%m-%d'))
        target_cont_duration['end_day'] = target_cont_duration['end_day'].apply(
            lambda x: datetime.strptime(str(x), '%Y-%m-%d'))

        # control 기간에서 diag 기간에 해당하는 날짜는 삭제
        # target_cont_duration
        target_cont_duration = pd.merge_asof(target_cont_duration.sort_values('end_day'),
                                             self.target_diag_duration.sort_values('start_day'),
                                             by='RN_INDI', left_on='end_day', right_on='start_day',
                                             suffixes=(None, '_'), allow_exact_matches=True)
        target_cont_duration = target_cont_duration.loc[lambda x: x['start_day'].gt(x['end_day_'])].drop(
            columns=['start_day_', 'end_day_'])

        num_con = pd.DataFrame(self.target_diag_duration.value_counts('RN_INDI')).reset_index()
        num_con.rename(columns={0: 'case'}, inplace=True)
        num_con['control'] = num_con['case'] * 2

        target_cont_duration.reset_index(inplace=True, drop=True)
        rnindi_list = target_cont_duration.RN_INDI.unique().tolist()
        rslts = pd.DataFrame(columns=target_cont_duration.columns.tolist())
        testlist = []
        for i in range(len(num_con)):
            rnindi = num_con.RN_INDI[i]
            num = num_con.control[i]
            try:
                tmp = target_cont_duration.query("RN_INDI == @rnindi")
                tmp = tmp.sample(n=num)
                rslts = rslts.append(tmp, ignore_index=True)
            except:
                pass
        target_cont_duration = rslts

        return target_cont_duration
