import pandas as pd
import pyodbc


def connect_db(research_dict):
    driver = "ODBC Driver 17 for SQL Server"
    server = "172.17.7.158,1433"

    # 접속 유저
    user = "vass_access"

    # 패스워드
    password = "password1!"

    # 데이터베이스명
    db = "VASS_DATA"

    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';UID=' + user + ';PWD=' + password)
    # cursor = cnxn.cursor()

    # table create

    # create result
    # check create
    # study design run code

    # scri
    # web db : 지정할수있어 디비저장

    # db > web db

    # dictionary reading
    use_start_date = int(research_dict['use_start_date'])
    use_end_date = int(research_dict['use_end_date'])
    research_start_date = int(research_dict['research_start_date'])
    research_end_date = int(research_dict['research_end_date'])

    table_HOI = 'SELECT * FROM dbo.example'
    table_HOI = pd.read_sql(table_HOI, cnxn)
    print('Table_HOI')

    vac = ('SELECT * FROM dbo.vcn2 where VCNYMD between {} and {}'.format(research_start_date, research_end_date))
    vac = pd.read_sql(vac, cnxn)

    print('vac')

    bfc = ('SELECT * from dbo.BNC where birthdate between {} and {}'.format(use_start_date, use_end_date))
    bfc = pd.read_sql(bfc, cnxn)
    print('bfc')

    table20 = (
        'SELECT * from dbo.T20 where MDCARE_STRT_DT between {} and {}'.format(research_start_date, research_end_date))
    table20 = pd.read_sql(table20, cnxn)
    HOI_patients_rnkey = table20.RN_KEY.tolist()
    print('table20')

    table30 = (
        'SELECT * from dbo.T30 where MDCARE_STRT_DT between {} and {}'.format(research_start_date, research_end_date))
    table30 = pd.read_sql(table30, cnxn)
    table30 = table30[table30.RN_KEY.isin(HOI_patients_rnkey)]
    print('table30')

    table60 = ('SELECT * from dbo.T60 where left(MDCARE_STRT_DT,8) between {} and {}'.format(research_start_date,
                                                                                             research_end_date))
    table60 = pd.read_sql(table60, cnxn)
    table60 = table60[table60.RN_KEY.isin(HOI_patients_rnkey)]
    print('table60')

    GNL2ATC = 'SELECT * from dbo.GNL2ATC_complete'
    GNL2ATC = pd.read_sql(GNL2ATC, cnxn)
    print('GNL2ATC')

    return table_HOI, vac, bfc, table20, table30, table60, GNL2ATC
#
# if __name__ == '__main__':
#     table_HOI, vac, bfc, table20, table30, table60, GNL2ATC = connect_db(research_dict)
