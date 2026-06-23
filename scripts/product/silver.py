from scripts.database import DatabaseFunctions

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def silver_product():
    table = """
            CREATE TABLE IF NOT EXISTS silver.silver.produtos (
                product_id VARCHAR PRIMARY KEY, nome_produto VARCHAR, categoria VARCHAR,
                margem_lucro DECIMAL(10,2), data_atualizacao TIMESTAMP
            );
        """

    query = """
            SELECT 
                (payload->>'product_id')::VARCHAR AS product_id,
                (payload->>'nome_produto')::VARCHAR AS nome_produto,
                (payload->>'categoria')::VARCHAR AS categoria,
                (((payload->>'preco_venda')::DECIMAL - (payload->>'preco_custo')::DECIMAL) / (payload->>'preco_custo')::DECIMAL * 100)::DECIMAL(10,2) AS margem_lucro,
                data_ingestao AS data_atualizacao
            FROM bronze.bronze.products
            ORDER BY data_ingestao ASC
        """

    bronze = DatabaseFunctions(db_con_string=BRONZE_CON,database='broze',schema='bronze',table= 'products')

    bronze.insert(sql_query=query,delivered_con_string=SILVER_CON,delivered_database='silver',delivered_schema='silver',delivered_table='produtos')