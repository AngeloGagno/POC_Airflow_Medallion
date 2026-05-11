from scripts.database import DatabaseFunctions
from scripts.checkpoint import get_checkpoint, commit_checkpoint

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def silver_sales():
    nome_checkpoint = "silver_sales_v1"
    db_silver = DatabaseFunctions(db_con_string=SILVER_CON, database='db_silver', schema='silver', table='vendas')
    db_silver.create("""
        CREATE TABLE IF NOT EXISTS db_alvo.silver.vendas (
            sale_id VARCHAR PRIMARY KEY, user_id VARCHAR, product_id VARCHAR, 
            quantidade INTEGER, metodo_pagamento VARCHAR,status_pedido VARCHAR, 
            data_venda TIMESTAMP, data_ingestao TIMESTAMP
        );
    """)

    # 2. Resgata a data de onde paramos
    ultima_data = get_checkpoint(checkpoint_name=nome_checkpoint)
    print(f"Marca d'água atual: {ultima_data}")
    
    db_bronze = DatabaseFunctions(db_con_string=BRONZE_CON, database='db_bronze', schema='bronze', table='sales')
    
    query_extracao = f"""
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
        from db_origem.bronze.sales
        WHERE (payload->>'data_venda')::TIMESTAMP > '{ultima_data}'
        ORDER BY (payload->>'data_venda')::TIMESTAMP ASC LIMIT 25000
    """

    query_max_date = f"SELECT MAX(data_venda) FROM ({query_extracao}) AS batch_limitado"
    
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
                delivered_table='vendas'
            )
            
            commit_checkpoint(
                checkpoint_name=nome_checkpoint,
                nova_data_maxima=nova_data_maxima
            )
            
        except Exception as e:
            print(f"❌ Erro durante a inserção. O checkpoint NÃO foi atualizado. Erro: {e}")
            raise e
    else:
        print("ℹ️ Nenhum dado novo encontrado para este lote.")



def consolidated_sales():
    nome_checkpoint = "silver_consolidated_sales_v1"
    db_silver = DatabaseFunctions(db_con_string=SILVER_CON, database='db_silver', schema='silver', table='vendas')
    db_silver.create("""
        CREATE TABLE IF NOT EXISTS db_alvo.silver.venda_consolidada (
            id_venda VARCHAR PRIMARY KEY,
            quantidade_produtos VARCHAR,
            metodo_pagamento VARCHAR, stauts_pedido VARCHAR, dt_venda TIMESTAMP,
            nome_cliente VARCHAR, email_cliente VARCHAR, dt_cadastro_cliente VARCHAR, nome_produto VARCHAR,
            categoria_produto VARCHAR, margem_lucro VARCHAR
        );
    """)

    # 2. Resgata a data de onde paramos
    ultima_data = get_checkpoint(checkpoint_name=nome_checkpoint)
    print(f"Marca d'água atual: {ultima_data}")
        
    query_extracao = f"""
        select 
            v.sale_id as id_venda,
            v.quantidade as quantidade_produtos ,v.metodo_pagamento, v.status_pedido,v.data_venda as dt_venda ,
            c.nome as nome_cliente, c.email as email_cliente, c.data_cadastro as dt_cadastro_cliente, 
            p.nome_produto,p.categoria as categoria_produto, p.margem_lucro 
        from db_origem.silver.vendas v
        left join db_origem.silver.clientes c on v.user_id = c.user_id
        left join db_origem.silver.produtos p on v.product_id = p.product_id
        WHERE v.data_venda > '{ultima_data}'
        ORDER BY v.data_venda ASC LIMIT 25000
    """

    query_max_date = f"SELECT MAX(dt_venda) FROM ({query_extracao}) AS batch_limitado"
    
    df_max = db_silver.select(sql_query=query_max_date, output_format='df')
    nova_data_maxima = df_max.iloc[0, 0]

    # ==========================================
    # 5. EXECUÇÃO TRANSACIONAL
    # ==========================================
    if nova_data_maxima is not None and str(nova_data_maxima) != 'NaT':
        try:
            db_silver.insert(
                sql_query=query_extracao,
                delivered_con_string=SILVER_CON,
                delivered_schema='silver',
                delivered_table='venda_consolidada'
            )
            
            commit_checkpoint(
                checkpoint_name=nome_checkpoint,
                nova_data_maxima=nova_data_maxima
            )
            
        except Exception as e:
            print(f"❌ Erro durante a inserção. O checkpoint NÃO foi atualizado. Erro: {e}")
            raise e
    else:
        print("ℹ️ Nenhum dado novo encontrado para este lote.")
