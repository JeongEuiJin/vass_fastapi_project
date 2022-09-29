from fastapi import APIRouter
from starlette.background import BackgroundTasks

from app.ML_analysis.main import ml_run
from app.ML_analysis.server_connect import connect_db
from app.models import GetParametersSCRI

router = APIRouter()


def run_ml_tasks(params):
    """
    최종 연구 코드 실행
    :param params: 웹서버에서 파라미터 받아온걸 받는다
    :return:
    """
    table_HOI, vac, bfc, table20, table30, table60, GNL2ATC = connect_db(params)
    results = ml_run(params, table_HOI, vac, bfc, table20, table30, table60, GNL2ATC)
    # results 를 디비에넣는걸 추가
    print(results)
    # SCRI function 추가



@router.post("/")
async def index(params: GetParametersSCRI, background_tasks: BackgroundTasks):
    """
    host 172.17.13.182
    :return:
    """

    print("params :: ", params)
    background_tasks.add_task(run_ml_tasks, params)

    return params
