from scripts.database import DatabaseFunctions
from scripts.checkpoint import get_checkpoint, commit_checkpoint

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def silver_sales():
    table ="""
            CREATE TABLE IF NOT EXISTS silver.silver.vendas (
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
                (payload->>'quantidade')::VARCHAR as quantidade,
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
            from bronze.bronze.sales
            WHERE (payload->>'data_venda')::TIMESTAMP > '{ultima_data}'
            ORDER BY (payload->>'data_venda')::TIMESTAMP ASC LIMIT 25000
        """
    database_bronze = DatabaseFunctions(db_con_string=BRONZE_CON,database='bronze',schema='bronze',table='sales')
    database_bronze.incremental_insert(target_con=SILVER_CON,target_db='silver',target_schema='silver',target_table='vendas',checkpoint_name='silver_vendas1',
                                    query_criacao_alvo=table,
                                    query_extracao_template=query,coluna_referencia_data='data_venda')
