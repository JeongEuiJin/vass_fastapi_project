from fastapi import APIRouter

from app.ML_analysis import server_connect as sc
from app.ML_analysis.main import ml_run
from app.models import GetParametersSCRI

router = APIRouter()


@router.post("/")
async def index(params: GetParametersSCRI):
    """
    host 172.17.13.182
    :return:
    """

    print("params :: ", params)
    table_HOI, vac, bfc, table20, table30, table60, GNL2ATC = sc.connect_db(params)
    ml_run(params, table_HOI, vac, bfc, table20, table30, table60, GNL2ATC)

    # SCRI function 추가
    pass


    return params
