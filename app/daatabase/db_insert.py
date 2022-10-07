from app.daatabase.connection import conn_web_db


def ml_insert(results):
    research_cnxn = conn_web_db()
    research_cursor = research_cnxn.cursor()

    print(results)
    for index, row in results.iterrows():
        research_cursor.execute(
            'INSERT INTO dbo.AnalysisResultsML (studydesign, vaccine, vcntime, hoidefn, calculated_date, fi_ratio, injected_case, risk_case) values (?,?,?,?,?,?,?,?)',
            row.studydesign, row.vaccine, row.vcntime, row.hoidefn, row.calculated_date, row.fi_ratio, row.injected_case, row.risk_case)

        research_cnxn.commit()
        research_cursor.close()

    print('AnalysisResultsML 로의 쓰기가 완료되었습니다.')