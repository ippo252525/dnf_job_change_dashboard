from pyneople.database_connecter import PostgreSQLConnecter, store_fame_data_to_mongodb, store_timeline_data_to_mongodb, mongodb_to_postgresql
from private_data import MONGO_CLIENT, PG_CONNECTION_DICT, API_KEY_LIST, DB_STRING
from pyneople.character import CharacterSearch
from sqlalchemy import create_engine
import pandas as pd

pg = PostgreSQLConnecter(PG_CONNECTION_DICT)

# Neople Open API 에서 유저 명성 검색을 통해 조회 할 수 있는 모든 캐릭터를 MongoDB에 저장합니다.
store_fame_data_to_mongodb(MONGO_CLIENT, "dnf", "fame_tb_20231121", API_KEY_LIST)

# 전처리 함수 정의
cs = CharacterSearch("")
def prepro(document):
    document = document['rows']
    data = []
    for character in document:
        cs.parse_data(character)
        data.append(tuple(
            [f"{cs.server_id} {cs.character_id}",
            cs.character_name,
            cs.level,
            cs.job_name,
            cs.job_grow_name,
            cs.fame]
        ))
    return data

# 테이블 생성
pg.create_table("fame_tb_20231121",
            ["total_id VARCHAR(43) NOT NULL PRIMARY KEY", 
            "character_name VARCHAR(16)",
            "level SMALLINT",
            "job_name VARCHAR(16)",
            "job_grow_name VARCHAR(16)",
            "fame INT"], 
            arg_drop=True)

# MongoDB에서 저장된 데이터를 전처리 후 PostgreSQL로 저장합니다.
mongodb_to_postgresql(pg, 'fame_tb_20231121', MONGO_CLIENT, 'dnf', 'fame_tb_20231121', prepro)

# 안개신 레이드 1주차 클리어 정보 확보를 위해 타임라인 데이터를 수집합니다.
query = \
"""
SELECT total_id
FROM fame_tb_20240502
WHERE fame > 58087
;"""
data = pg.fetch(query)
data = [character[0] for character in data]
store_timeline_data_to_mongodb(MONGO_CLIENT, 'dnf', 'timeline_tb_20240502', API_KEY_LIST, data, "2024-04-25 12:00","2024-05-02 05:30")

pg.create_table("timeline_tb_20240502",
            ["total_id VARCHAR(43)",
             "timeline_code SMALLINT",
             "timeline_date TIMESTAMP",
             "timeline_data TEXT"
             ], 
            arg_drop=True)

def prepro(document):
    data = []
    for timeline in document.get("timeline"):
        data.append((document['total_id'], timeline['code'], timeline['date'], str(timeline['data'])))
    return data
    
mongodb_to_postgresql(pg,'timeline_tb_20240502', MONGO_CLIENT, 'dnf', 'timeline_tb_20240502', prepro)



# 저장된 데이터를 통해 2023년 11월 21일 과 2023년 4월 23일에 동일한 캐릭터가 다른 전직을 가지는 경우를 가져옵니다.
engine = create_engine(DB_STRING)
query = \
"""
SELECT
    fame_tb_20231121.total_id AS total_id,
    fame_tb_20231121.job_name AS job_name,
    fame_tb_20231121.job_grow_name AS old_job_grow_name,
    fame_tb_20231121.fame AS old_fame,
    fame_tb_20240423.job_grow_name AS new_job_grow_name,
    fame_tb_20240423.fame AS new_fame,
    first_clear.timeline_data
FROM 
    fame_tb_20231121
INNER JOIN 
    fame_tb_20240423
    ON fame_tb_20231121.total_id = fame_tb_20240423.total_id
    AND fame_tb_20231121.level = 110
    AND fame_tb_20240423.job_name = fame_tb_20231121.job_name
    AND fame_tb_20240423.job_grow_name <> fame_tb_20231121.job_grow_name
LEFT JOIN 
    timeline_tb_20240502 AS first_clear
    ON fame_tb_20231121.total_id = first_clear.total_id
    AND first_clear.timeline_code = 210;
"""
df = pd.read_sql_query(query, engine)
df['first_clear'] = df['timeline_data'].notna()
df.drop(columns=['timeline_data'])
df.to_excel('example.xlsx', index=False)
