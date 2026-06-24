from scripts.database import DatabaseFunctions

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def silver_sales():
    table ="""
            CREATE TABLE IF NOT EXISTS db_silver.silver.vendas (
                sale_id VARCHAR PRIMARY KEY, user_id VARCHAR, product_id VARCHAR, 
                quantidade INTEGER, metodo_pagamento VARCHAR,status_pedido VARCHAR, 
                data_venda TIMESTAMP, data_ingestao TIMESTAMP
            );
        """

    query = """
                    select 
                (payload->>'sale_id')::VARCHAR as sale_id,
                (payload->>'user_id')::VARCHAR as user_id,
                (payload->>'product_id')::VARCHAR as product_id,
                (payload->>'quantidade')::INTEGER as quantidade,
                case
                    when (payload->>'metodo_pagamento')::INTEGER = 1 then 'dinheiro'
                    when (payload->>'metodo_pagamento')::INTEGER = 2 then 'credito'
                    when (payload->>'metodo_pagamento')::INTEGER = 3 then 'debito'
                    else 'pagamento nao identificado'
                end as metodo_pagamento,
                case
                    when (payload->>'status_pedido')::INTEGER = 1 then 'confirmado'
                    when (payload->>'status_pedido')::INTEGER = 2 then 'em transito'
                    when (payload->>'status_pedido')::INTEGER = 3 then 'entregue'
                    else 'status nao identificado'
                end as status_pedido,
                (payload->>'data_venda')::TIMESTAMP as data_venda,
                data_ingestao
            from db_bronze.bronze.sales
            WHERE (payload->>'data_venda')::TIMESTAMP > '{ultima_data}'
            ORDER BY (payload->>'data_venda')::TIMESTAMP ASC LIMIT 25000
        """
    
    query_upsert_pura = """
        INSERT INTO silver.vendas (
            sale_id, user_id, product_id, quantidade, 
            metodo_pagamento, status_pedido, data_venda, data_ingestao
        )
        SELECT 
            sale_id, user_id, product_id, quantidade, 
            metodo_pagamento, status_pedido, data_venda, data_ingestao
        FROM silver.vendas_stg
        
        ON CONFLICT (sale_id) 
        DO UPDATE SET 
            user_id = EXCLUDED.user_id,
            product_id = EXCLUDED.product_id,
            quantidade = EXCLUDED.quantidade,
            metodo_pagamento = EXCLUDED.metodo_pagamento,
            status_pedido = EXCLUDED.status_pedido,
            data_venda = EXCLUDED.data_venda,
            data_ingestao = EXCLUDED.data_ingestao;
    """
    database_bronze = DatabaseFunctions(db_con_string=BRONZE_CON,database='db_bronze',schema='bronze',table='sales')
    database_bronze.incremental_upsert(target_con=SILVER_CON,target_db='db_silver',target_schema='silver',target_table='vendas',checkpoint_name='silver_vendas5',
                                    query_criacao_alvo=table,
                                    query_extracao_template=query,query_upsert_postgres=query_upsert_pura, coluna_referencia_data='data_venda')
