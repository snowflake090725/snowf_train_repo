import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import json


def sp_create_table_from_stage(session: snowpark.Session, param:dict):
    data = get_data_from_stage(session, param['SCHEMA'], param['STAGE'])
    table_data = get_dataframe(session, data)
    create_table(session, param['TABLE_NAME'], table_data)
    return table_data
    
def get_data_from_stage(session, schema, stage_name):
    dataset = []
    data = session.sql(f"""select $1 as raw_data from '@{schema}.{stage_name}'""").collect()
    # dataset = [record for record in data]
    for record in data:
        d = json.loads(record['RAW_DATA'])
        res_data = dict(list(d.items()))
        dataset.append(res_data)
    return dataset


def get_dataframe(session, parquet_data):
    dataframe = session.create_dataframe(parquet_data)
    print(dataframe.columns)
    return dataframe

def create_table(session, table_name, data):
    dataframe = data.write.mode("overwrite").save_as_table({table_name})
    return dataframe
