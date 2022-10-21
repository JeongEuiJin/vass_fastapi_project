from datetime import datetime
from app.daatabase.connection import conn_web_db


def ml_insert(results_ml):
    research_cnxn = conn_web_db()
    research_cursor = research_cnxn.cursor()
    date = datetime.now()

    for index, row in results_ml.iterrows():
        research_cursor.execute('''select id from dbo.Vaccine where vaccine_target_id={} and vcn_time={}'''.format(row.vaccine, row.vcntime))
        vac_id = research_cursor.fetchall()[0][0]

        research_cursor.execute('''select id from dbo.VaccineHOIdefn where vaccine_id={} and hoidefn_id={}'''.format(vac_id, row.hoidefn))
        vaccinehoidefn_id = research_cursor.fetchall()[0][0]

        research_cursor.execute(
            'INSERT INTO dbo.AnalysisResultsML (is_deleted, created_time, updated_time, calculated_time, injected_case, risk_case, fi_ratio, vaccinehoidefn_id, studydesign_id) values (?,?,?,?,?,?,?,?,?)',
            False, date, date, row.calculated_date, row.injected_case, row.risk_case, row.fi_ratio, vaccinehoidefn_id, row.studydesign
        )

        research_cursor.execute('''UPDATE dbo.StudyDesign SET study_design_run = 'true' where id={}'''.format(row.studydesign))

        research_cnxn.commit()
    research_cursor.close()

    print('AnalysisResultsML 로의 쓰기가 완료되었습니다.')

def scri_insert(results):
    research_cnxn = conn_web_db()
    research_cursor = research_cnxn.cursor()
    date = datetime.now()

    for index, row in results.iterrows():
        # Vaccine에서 백신 종류랑 차수를 불러와야 됨
        research_cursor.execute('''select id from dbo.Vaccine where vaccine_target_id={} and vcn_time={}'''.format(row.vaccine, row.vcntime))
        vac_id = research_cursor.fetchall()[0][0]

        research_cursor.execute('''select id from dbo.VaccineHOIdefn where vaccine_id={} and hoidefn_id={}'''.format(vac_id, row.hoidefn))
        vaccinehoidefn_id = research_cursor.fetchall()[0][0]

        research_cursor.execute(
            'INSERT INTO dbo.AnalysisResultsSCRI (is_deleted, created_time, updated_time, calculated_time, injected_case, risk_case, con_case, irr, irr_cutoff, vaccinehoidefn_id, studydesign_id) values (?,?,?,?,?,?,?,?,?,?,?)',
            False, date, date, row.calculated_date, row.injected_case, row.risk_case, row.con_case, row.irr, 1, vaccinehoidefn_id, row.studydesign
        )
        research_cursor.execute(
            '''UPDATE dbo.StudyDesign SET study_design_run = 'true' where id={}'''.format(row.studydesign))

        research_cnxn.commit()
    research_cursor.close()

    print('AnalysisResultsSCRI 로의 쓰기가 완료되었습니다.')

def scri_sex_insert(results):
    research_cnxn = conn_web_db()
    research_cursor = research_cnxn.cursor()
    date = datetime.now()

    for index, row in results.iterrows():
        research_cursor.execute('''select id from dbo.Vaccine where vaccine_target_id={} and vcn_time={}'''.format(row.vaccine, row.vcntime))
        vac_id = research_cursor.fetchall()[0][0]

        research_cursor.execute('''select id from dbo.VaccineHOIdefn where vaccine_id={} and hoidefn_id={}'''.format(vac_id, row.hoidefn))
        vaccinehoidefn_id = research_cursor.fetchall()[0][0]

        research_cursor.execute(
            'INSERT INTO dbo.AnalysisResultsSex (is_deleted, created_time, updated_time, calculated_time, sex, irr, irr_cutoff, vaccinehoidefn_id, studydesign_id) values (?,?,?,?,?,?,?,?,?)',
            False, date, date, row.calculated_date, row.sex, row.irr, 1, vaccinehoidefn_id, row.studydesign
        )

        research_cursor.execute(
            '''UPDATE dbo.StudyDesign SET study_design_run = 'true' where id={}'''.format(row.studydesign))

        research_cnxn.commit()
    research_cursor.close()

    print('AnalysisResultsSex 로의 쓰기가 완료되었습니다.')

