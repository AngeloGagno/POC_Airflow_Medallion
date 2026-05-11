from scripts.database import DatabaseFunctions
from scripts.checkpoint import get_checkpoint, commit_checkpoint

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def silver_products():
    nome_checkpoint = "silver_produtos_v1"

    # 1. Garante a estrutura do destino
    db_silver = DatabaseFunctions(db_con_string=SILVER_CON, database='db_silver', schema='silver', table='produtos')
    db_silver.create("""
        CREATE TABLE IF NOT EXISTS db_alvo.silver.produtos (
            product_id VARCHAR PRIMARY KEY, nome_produto VARCHAR, categoria VARCHAR,
            margem_lucro DECIMAL(10,2), data_atualizacao TIMESTAMP
        );
    """)

    # 2. Resgata a data de onde paramos
    ultima_data = get_checkpoint(checkpoint_name=nome_checkpoint)
    print(f"Marca d'água atual: {ultima_data}")
    
    db_bronze = DatabaseFunctions(db_con_string=BRONZE_CON, database='db_bronze', schema='bronze', table='products')
    
    query_extracao = f"""
        SELECT 
            (payload->>'product_id')::VARCHAR AS product_id,
            (payload->>'nome_produto')::VARCHAR AS nome_produto,
            (payload->>'categoria')::VARCHAR AS categoria,
            (((payload->>'preco_venda')::DECIMAL - (payload->>'preco_custo')::DECIMAL) / (payload->>'preco_custo')::DECIMAL * 100)::DECIMAL(10,2) AS margem_lucro,
            data_ingestao AS data_atualizacao
        FROM db_origem.bronze.products
        ORDER BY data_ingestao ASC
    """

    query_max_date = f"SELECT MAX(data_atualizacao) FROM ({query_extracao}) AS batch_limitado"
    
    df_max = db_bronze.select(sql_query=query_max_date, output_format='df')
    nova_data_maxima = df_max.iloc[0, 0]
    
    if nova_data_maxima is not None and str(nova_data_maxima) != 'NaT':
        try:
            db_bronze.insert(
                sql_query=query_extracao,
                delivered_con_string=SILVER_CON,
                delivered_schema='silver',
                delivered_table='produtos'
            )
            
            commit_checkpoint(
                checkpoint_name=nome_checkpoint,
                nova_data_maxima=nova_data_maxima
            )
            
        except Exception as e:
            print(f"❌ Erro durante a inserção. O checkpoint NÃO foi atualizado. Erro: {e}")
            raise e
    else:
        print("ℹ️ Nenhum dado novo encontrado na Bronze para este lote.")