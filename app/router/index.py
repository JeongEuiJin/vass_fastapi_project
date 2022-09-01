from fastapi import APIRouter

from app.models import GetParametersSCRI

router = APIRouter()


@router.post("/")
async def index(params: GetParametersSCRI):
    """

    :return:
    """
    print("params :: ", params)
    return params