from database import DatabaseFunctions

GOLD_CON = "postgresql://admin:password@pg_gold:5432/db_gold"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def gold_vendas():
    print("Iniciando processamento da Gold Vendas...")
    query_criacao = """
        CREATE TABLE IF NOT EXISTS db_alvo.gold.vendas (
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
        FROM db_origem.silver.venda_consolidada
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

    # 3. CHAMADA DO FRAMEWORK
    executar_carga_incremental(
        # Origem (A sua tabela base na Silver)
        source_con=SILVER_CON,
        source_db='db_silver',
        source_schema='silver',
        source_table='venda_consolidada',
        
        # Destino (A nova tabela agrupada na Gold)
        target_con=GOLD_CON,
        target_db='db_gold',
        target_schema='gold',
        target_table='vendas',
        
        # Parâmetros lógicos
        checkpoint_name='checkpoint_gold_vendas_v4',
        query_criacao_alvo=query_criacao,
        query_extracao_template=query_extracao,
        coluna_referencia_data='dt_venda'
    )

if __name__ == "__main__":
    gold_vendas()