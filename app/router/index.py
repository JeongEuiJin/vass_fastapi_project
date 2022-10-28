from fastapi import APIRouter
from starlette.background import BackgroundTasks

from app.ML_analysis.main import ml_run
from app.ML_analysis.server_connect import connect_db
from app.daatabase.db_insert import ml_insert
from app.daatabase.db_insert import scri_insert
from app.daatabase.db_insert import scri_sex_insert
from app.models import GetParametersSCRI
from app.ML_analysis.vaccine_scri_ss_CMI import SCRI

router = APIRouter()


def run_ml_tasks(params: dict):
    """
    최종 연구 코드 실행
    :param params: 웹서버에서 파라미터 받아온걸 받는다
    :return:
    """
    table_HOI, vac, bfc, table20, table30, table60, GNL2ATC, death = connect_db(params)
    print ('SCRI 분석 시작')
    scri_results, scri_sex_results = SCRI(params, table_HOI, bfc, death, vac)
    print(scri_results)
    scri_insert(scri_results)
    if len(scri_sex_results) != 0:
        print(scri_sex_results)
        scri_sex_insert(scri_sex_results)
    else:
        print('성별 분석 진행하지 않음.')
        pass

    table_HOI, vac, bfc, table20, table30, table60, GNL2ATC, death = connect_db(params)
    ml_results = ml_run(params, table_HOI, vac, bfc, table20, table30, table60, GNL2ATC)
    print (ml_results)
    ml_insert(ml_results)

    print ('분석이 전부 완료되었습니다.')





@router.post("/")
async def index(params: GetParametersSCRI, background_tasks: BackgroundTasks):
    """
    host 172.17.13.182
    :return:
    """

    print("request!! run ml:: ", params)
    background_tasks.add_task(run_ml_tasks, params)

    return {"background_task": True}
