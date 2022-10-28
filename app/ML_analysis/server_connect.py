import pandas as pd
from time import sleep

from app.daatabase.connection import conn_db


def connect_db(research_dict: dict):
    # dictionary reading: 연구디자인 페이지에서 각 항목을 클릭했을 때 나오는 딕셔너리들 중 일부를 불러옵니다.
    use_start_date = int(research_dict.use_start_date)
    use_end_date = int(research_dict.use_end_date)
    research_start_date = int(research_dict.research_start_date)
    research_end_date = int(research_dict.research_end_date)
    vcncd = int(research_dict.vaccine_target_id)  # 백신종류
    vcntme = int(research_dict.vcntime)  # 백신차수

    # 조작적 정의 sql query 읽기
    hoi_sql = research_dict.operationaldefinition_query
    hoi_sql = hoi_sql.replace('\n',' ')
    print('조작적 정의 데이터 추출 중')
    sleep(120)

    # 조작적 정의 DB 연결
    research_cnxn = conn_db('VASS_DATA')
    data_cnxn = conn_db('VASS_DATA')
    research_cursor = research_cnxn.cursor()

    with research_cnxn:
        research_cursor.execute(hoi_sql)
        query="select top 1 table_name from (select schema_name(schema_id) as schema_name, name as table_name,create_date from sys.tables where create_date > DATEADD(DAY, -30, CURRENT_TIMESTAMP))v order by create_date desc;"
        research_cursor.execute(query)
        table_HOI_name = research_cursor.fetchall()[0][0]
        table_HOI = 'SELECT * FROM dbo.{} where MDCARE_STRT_DT between {} and {}'.format(table_HOI_name, research_start_date,
                                                                                         research_end_date)

        # with research_cnxn:
        # table_HOI = 'SELECT * FROM dbo.tmp_core_hsptz_335552554 where MDCARE_STRT_DT between {} and {}'.format(research_start_date, research_end_date)  # 선택한 HOI에 해당하는 쿼리가 돌아간 결과가 저장된 테이블명을 넣어주시면 됩니다.
        table_HOI = pd.read_sql(table_HOI, research_cnxn)
        table_HOI.rename(columns={'rn_indi':'RN_INDI','rn_key':'RN_KEY'}, inplace=True)
        drop_table_HOI = 'drop table dbo.{}'.format(table_HOI_name)
        research_cursor.execute(drop_table_HOI)
        research_cursor.commit()
        print('Table_HOI')

    with data_cnxn:
        # 접종데이터 상의 VCNCD를 vaccine target 테이블의 VCNCD로 맞춰주는 작업
        vac_mapping = ('SELECT * FROM dbo.vcn_map where VaccineTargetID={}'.format(vcncd))
        vac_mapping = pd.read_sql(vac_mapping, data_cnxn)
        vac_mapping = vac_mapping.VCNCD.tolist()
        vac_mapping = list(map(int, vac_mapping))

        vac = ('SELECT * FROM dbo.vcn_more where VCNYMD between {} and {}'.format(research_start_date, research_end_date))
        # vac = ('SELECT * FROM dbo.vcn2')
        vac = pd.read_sql(vac, data_cnxn)

        # vac_mapping에 있는 값이 들어있는 기록만 추출한 후, 차수 만족하는 기록만 추출함
        vac = vac[vac['VCNCD'].isin(vac_mapping)]

        # 인플루엔자는 VCNTME을 전부 1로 수정
        vac.loc[vac.VCNCD == 901, 'VCNTME'] = 1
        vac.loc[vac.VCNCD == 902, 'VCNTME'] = 1

        # Tdap, Td는 VCNTME을 6으로 수정
        vac.loc[vac.VCNCD == 306, 'VCNTME'] = 6
        vac.loc[vac.VCNCD == 302, 'VCNTME'] = 6

        vac = vac.query('VCNTME=={}'.format(vcntme))
        print('vac')

        # bfc table 추출
        bfc = ('SELECT * from dbo.BNC where birthdate between {} and {}'.format(use_start_date, use_end_date))
        bfc = pd.read_sql(bfc, data_cnxn)
        print('bfc')

        # 20 table 추출
        table20 = (
            'SELECT * from dbo.T20 where MDCARE_STRT_DT between {} and {}'.format(research_start_date, research_end_date))
        table20 = pd.read_sql(table20, data_cnxn)
        HOI_patients_rnkey = table20.RN_KEY.tolist()
        print('table20')

        # 30 table 추출
        table30 = ('SELECT * from dbo.T30 where MDCARE_STRT_DT between {} and {}'.format(research_start_date, research_end_date))
        table30 = pd.read_sql(table30, data_cnxn)
        table30 = table30[table30.RN_KEY.isin(HOI_patients_rnkey)]
        print('table30')

        # 60 table 추출
        table60 = ('SELECT * from dbo.T60 where left(MDCARE_STRT_DT,8) between {} and {}'.format(research_start_date, research_end_date))
        table60 = pd.read_sql(table60, data_cnxn)
        table60 = table60[table60.RN_KEY.isin(HOI_patients_rnkey)]
        print('table60')

        # GNL2ATC 불러오기 추출
        GNL2ATC = 'SELECT * from dbo.GNL2ATC_complete'
        GNL2ATC = pd.read_sql(GNL2ATC, data_cnxn)
        print('GNL2ATC')

        # 사망테이블 불러오기
        death = 'SELECT * from dbo.BND'
        death = pd.read_sql(death, data_cnxn)
        print('BND')

    return table_HOI, vac, bfc, table20, table30, table60, GNL2ATC, death
