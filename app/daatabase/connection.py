import json

import pyodbc

from app.common.config import conf


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

    driver = database_config['DRIVER']  # DB DRIVER ex) mssql - 'ODBC Driver 17 for SQL Server'
    server = database_config['HOST']  # HOST
    port = database_config['PORT']  # PORT 포트가 있다면 연결해야될것으로 보임

    # 접속 유저
    user = database_config['USER']

    # 패스워드
    password = database_config['PASSWORD']

    # DB 연결
    _url = 'DRIVER={' + driver + '};SERVER=' + server + ',' + port + ';DATABASE=' + _db_name + ';UID=' + user + ';PWD=' + password
    return pyodbc.connect(_url)


def conn_web_db():
    """
    결과 테이블
    :return:
    """
    c = conf()

    # secret json file load
    config_secret_setting = json.loads(open(c.CONFIG_SETTING_FILE).read())
    # database settings config
    database_config = config_secret_setting['django']['databases']

    driver = database_config['DRIVER']
    server = database_config['HOST']
    database = database_config['NAME']
    # 접속 유저
    user = database_config['USER']

    # 패스워드
    password = database_config['PASSWORD']
    _url = 'DRIVER={' + driver + '};SERVER=' + server + ';DATABASE=' + database + ';UID=' + user + ';PWD=' + password
    return pyodbc.connect(_url)
