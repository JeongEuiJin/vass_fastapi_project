from typing import Union

import uvicorn
from fastapi import FastAPI, Query
from pydantic import BaseModel

from app.common.config import conf
from app.router import index


def create_app():
    c = conf()
    app = FastAPI()
    # 데이타베이스  이니셜라이즈
    # 레디스 이니셜라이즈
    # 미들웨어 정의
    # 라우터 정의
    app.include_router(index.router)
    return app


app = create_app()
#
# if __name__ == '__main__':
#     uvicorn.run('main:app', host='0.0.0.0', port=8000, reload=True)
