from scripts.database import DatabaseFunctions

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
table = '''CREATE TABLE IF NOT EXISTS bronze.bronze.sales (
    payload JSONB,
    data_ingestao TIMESTAMP);
)'''

query = '''Select payload, data_ingestao
        where data_ingestao > '{ultima_data}' 
        limit 10000'''

raw = DatabaseFunctions(db_con_string=BRONZE_CON,database='raw',schema='raw',table='raw_sales')
raw.incremental_insert(target_con=BRONZE_CON,target_db='bronze',target_schema='bronze',target_table='sales',
                       checkpoint_name='bronze_sales',query_criacao_alvo=table, query_extracao_template= query, coluna_referencia_data= 'data_ingestao')