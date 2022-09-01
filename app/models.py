from pydantic import BaseModel


class GetParametersSCRI(BaseModel):
    title: str
    degree: int
