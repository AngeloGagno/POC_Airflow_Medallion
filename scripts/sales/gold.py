from database import DatabaseFunctions

GOLD_CON = "postgresql://admin:password@pg_gold:5432/db_gold"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def gold_vendas():
    print("Iniciando processamento da Gold Vendas...")
    query_criacao = """
        CREATE TABLE IF NOT EXISTS gold.gold.vendas (
            nome_produto VARCHAR, 
            total_vendido INTEGER, 
            margem_lucro FLOAT,
            dt_venda TIMESTAMP, 
            status_pedido VARCHAR, 
            nome_cliente VARCHAR
        );
    """

    query_extracao = """
        SELECT 
            nome_produto,
            sum(quantidade_produtos::integer) as total_vendido,
            avg(margem_lucro::float) as margem_lucro,
            date_trunc('day', dt_venda) as dt_venda,
            stauts_pedido as status_pedido, 
            nome_cliente
        FROM silver.silver.venda_consolidada
        WHERE nome_produto IS NOT NULL
          AND dt_venda >= '{ultima_data}'::timestamp
          AND dt_venda < '{ultima_data}'::timestamp + INTERVAL '1 day'
        GROUP BY 
            date_trunc('day', dt_venda),
            stauts_pedido,
            nome_produto, 
            nome_cliente
        ORDER BY date_trunc('day', dt_venda)
    """

    
    
