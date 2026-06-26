from scripts.database import DatabaseFunctions

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"

def bronze_sales():
    table = '''CREATE TABLE IF NOT EXISTS db_bronze.bronze.sales (
        payload JSON,
        data_ingestao TIMESTAMP);
    '''

    query = '''Select payload, data_ingestao
            from db_bronze.raw.raw_sales
            where (payload->>'data_venda')::TIMESTAMP > '{ultima_data}'     
            '''

    raw = DatabaseFunctions(db_con_string=BRONZE_CON,database='db_bronze',schema='raw',table='raw_sales')
    raw.incremental_insert(target_con=BRONZE_CON,target_db='db_bronze',target_schema='bronze',target_table='sales',
                        checkpoint_name='bronze_sales5',query_criacao_alvo=table, query_extracao_template= query, coluna_referencia_data= 'data_ingestao')