from app.daatabase.connection import conn_web_db


def ml_insert(results):
    research_cnxn = conn_web_db()
    research_cursor = research_cnxn.cursor()

    print(results)
    for index, row in results.iterrows():
        research_cursor.execute('''select id from dbo.VaccineHOIdefn where vaccine_id={} and hoidefn_id={}'''.format(row.vaccine, row.hoidefn))
        vaccinehoidefn_id = research_cursor.fetchall()[0][0]

        research_cursor.execute(
            'INSERT INTO dbo.AnalysisResultsML (is_deleted, studydesign_id, vaccinehoidefn_id, calculated_time, fi_ratio, injected_case, risk_case) values (?,?,?,?,?,?)',
            False, row.studydesign, vaccinehoidefn_id, row.calculated_date, row.fi_ratio, row.injected_case, row.risk_case
        )

        research_cnxn.commit()
        research_cursor.close()

    print('AnalysisResultsML 로의 쓰기가 완료되었습니다.')