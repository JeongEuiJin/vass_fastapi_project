import json

import pyodbc

from app.common.config import conf


# 저희의 경우 MSSQL server를 활용하기에 해당 드라이버를 활용했으나, 귀사의 서버 드라이버를 적용해 주시면 됩니다.
def conn_db(_db_name):
    """
    연결할 디비 설정
    사용시 with문 사용
    :param _db_name: 사용할 db 의 이름
    :return: connect
    """
    c = conf()

    # secret json file load
    config_secret_setting = json.loads(open(c.CONFIG_SETTING_FILE).read())
    # database settings config
    database_config = config_secret_setting['fastapi']['databases']

    driver = database_config['driver']  # DB DRIVER ex) mssql - 'ODBC Driver 17 for SQL Server'
    server = database_config['HOST']  # HOST
    port = database_config['PORT']  # PORT 포트가 있다면 연결해야될것으로 보임

    # 접속 유저
    user = database_config['USER']

    # 패스워드
    password = database_config['USER']

    # DB 연결
    _url = 'DRIVER={' + driver + '};SERVER=' + server + ';DATABASE=' + _db_name + ';UID=' + user + ';PWD=' + password
    return pyodbc.connect(_url)
