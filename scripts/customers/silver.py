from scripts.database import DatabaseFunctions


BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def silver_customers():
    nome_checkpoint = "silver_clientes_v1"
    table = """
        CREATE TABLE IF NOT EXISTS silver.silver.clientes (
            user_id VARCHAR PRIMARY KEY,
            nome VARCHAR,
            email VARCHAR,
            data_cadastro TIMESTAMP,
            data_ingestao TIMESTAMP          
        );
    """)
    query = """
        SELECT 
            (payload->>'user_id')::VARCHAR AS user_id,
            TRIM(REGEXP_REPLACE(
                (payload->>'nome')::VARCHAR, 
                '^(?:sr|srs|sra|sras|srta|srtas|senhor|senhores|senhora|senhoras|senhorita|senhoritas|dr|drs|dra|dras|doutor|doutores|doutora|doutoras|prof|profa|professor|professora|dom|dona)\\.?\\s+', 
                '', 
                'ig'
            )) AS nome,
            (payload->>'email')::VARCHAR AS email,
            (payload->>'data_cadastro')::VARCHAR AS data_cadastro,
            data_ingestao
        FROM bronze.bronze.users
        WHERE (payload->>'data_venda')::TIMESTAMP > '{ultima_data}'
        ORDER BY data_cadastro ASC LIMIT 10000
    """
    bronze = DatabaseFunctions(db_con_string=BRONZE_CON,database='bronze',schema='bronze',table='users')
    bronze.incremental_upsert(target_con=SILVER_CON,target_db='silver',target_schema='silver',target_table='clientes', checkpoint_name=nome_checkpoint, query_criacao_alvo=table, query_extracao_template=query
                            , coluna_referencia_data='data_venda', coluna_merge='user_id', condition_merge='UPDATE')