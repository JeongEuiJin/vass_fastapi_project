from fastapi import APIRouter

from app.ML_analysis.main import ml_run
from app.ML_analysis.server_connect import connect_db
from app.models import GetParametersSCRI

router = APIRouter()


@router.post("/")
async def index(params: GetParametersSCRI):
    """
    host 172.17.13.182
    :return:
    """

    print("params :: ", params)
    table_HOI, vac, bfc, table20, table30, table60, GNL2ATC = connect_db(params)
    results = ml_run(params, table_HOI, vac, bfc, table20, table30, table60, GNL2ATC)
    print(results)

    # SCRI function 추가
    pass


    return params
