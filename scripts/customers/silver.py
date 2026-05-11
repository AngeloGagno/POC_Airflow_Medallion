from scripts.database import DatabaseFunctions
from scripts.checkpoint import get_checkpoint, commit_checkpoint

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def silver_customers():
    nome_checkpoint = "silver_clientes_v1"

    # 1. Garante a estrutura do destino
    db_silver = DatabaseFunctions(db_con_string=SILVER_CON, database='db_silver', schema='silver', table='clientes')
    db_silver.create("""
        CREATE TABLE IF NOT EXISTS db_alvo.silver.clientes (
            user_id VARCHAR PRIMARY KEY,
            nome VARCHAR,
            email VARCHAR,
            data_cadastro TIMESTAMP,
            data_ingestao TIMESTAMP          
        );
    """)

    # 2. Resgata a data de onde paramos
    ultima_data = get_checkpoint(checkpoint_name=nome_checkpoint)
    print(f"Marca d'água atual: {ultima_data}")
    
    db_bronze = DatabaseFunctions(db_con_string=BRONZE_CON, database='db_bronze', schema='bronze', table='users')
    
    query_extracao = f"""
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
        FROM db_origem.bronze.users
        WHERE (payload->>'data_venda')::TIMESTAMP > '{ultima_data}'
        ORDER BY data_cadastro ASC LIMIT 10000
    """

    query_max_date = f"SELECT MAX(data_cadastro) FROM ({query_extracao}) AS batch_limitado"
    
    df_max = db_bronze.select(sql_query=query_max_date, output_format='df')
    nova_data_maxima = df_max.iloc[0, 0]

    # ==========================================
    # 5. EXECUÇÃO TRANSACIONAL
    # ==========================================
    if nova_data_maxima is not None and str(nova_data_maxima) != 'NaT':
        try:
            db_bronze.insert(
                sql_query=query_extracao,
                delivered_con_string=SILVER_CON,
                delivered_schema='silver',
                delivered_table='clientes'
            )
            
            commit_checkpoint(
                checkpoint_name=nome_checkpoint,
                nova_data_maxima=nova_data_maxima
            )
            
        except Exception as e:
            print(f"Erro durante a inserção. O checkpoint NÃO foi atualizado. Erro: {e}")
            raise e
    else:
        print("ℹ️ Nenhum dado novo encontrado na Bronze para este lote.")