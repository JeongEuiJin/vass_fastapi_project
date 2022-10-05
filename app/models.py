from typing import Optional

from pydantic import BaseModel


class GetParametersSCRI(BaseModel):
    use_start_date: str
    use_end_date: str
    research_start_date: Optional[str] = None
    research_end_date: Optional[str] = None
    vaccine_target_id: Optional[int] = None
    hoidefn: Optional[int] = None
    vcntime: Optional[int] = None
    studydesignid: Optional[int] = None
    age_type: Optional[str] = None
    age_select_start: Optional[int] = None
    age_select_end: Optional[int] = None
    operationaldefinition_query: Optional[str] = None
    gap_era: Optional[int] = None
    scri_risk_window_start: Optional[int] = None
    scri_risk_window_end: Optional[int] = None
    scri_con_window_start_1: Optional[int] = None
    scri_con_window_end_1: Optional[int] = None
    scri_con_window_start_2: Optional[int] = None
    scri_con_window_end_2: Optional[int] = None
    scri_gender: Optional[int] = None
    scri_agegroup: Optional[int] = None
    ml_window_size: Optional[int] = None
