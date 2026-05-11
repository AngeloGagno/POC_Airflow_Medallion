from scripts.database import DatabaseFunctions
from scripts.checkpoint import get_checkpoint, commit_checkpoint
GOLD_CON = "postgresql://admin:password@pg_gold:5432/db_gold"
SILVER_CON = "postgresql://admin:password@pg_silver:5432/db_silver"

def gold_vendas():
    checkpoint = 'checkpoint_gold_vendas_v4'
    db_gold = DatabaseFunctions(db_con_string=GOLD_CON,database='db_gold',schema='gold',table='vendas')
    db_gold.create('''CREATE TABLE IF NOT EXISTS db_alvo.gold.vendas (nome_produto VARCHAR, total_vendido INTEGER, margem_lucro FLOAT,
                   dt_venda TIMESTAMP, status_pedido VARCHAR, nome_cliente VARCHAR);''')
    
    ultima_data = get_checkpoint(checkpoint)
    print(f"Marca d'água atual: {ultima_data}")
    db_silver = DatabaseFunctions(db_con_string=SILVER_CON,database='db_silver',schema='silver',table='sales')
    query_extracao = f"""select 
    nome_produto,
    sum(quantidade_produtos::integer) as total_vendido,
    avg(margem_lucro::float) as margem_lucro,
    date_trunc('day',dt_venda) as dt_venda,stauts_pedido as status_pedido, nome_cliente
    from db_origem.silver.venda_consolidada vc 
    where nome_produto is not null
    and dt_venda > '{ultima_data}'
    group by date_trunc('day',dt_venda),stauts_pedido,nome_produto, nome_cliente
    order by date_trunc('day',dt_venda)
    limit 500
    """

    query_max_date = f"SELECT MAX(dt_venda) FROM ({query_extracao}) AS batch_limitado"

    df_max = db_silver.select(sql_query=query_max_date, output_format='df')
    nova_data_maxima = df_max.iloc[0, 0]

    if nova_data_maxima is not None and str(nova_data_maxima) != 'NaT':
        try:
            db_silver.insert(
                sql_query=query_extracao,
                delivered_con_string=GOLD_CON,
                delivered_schema='gold',
                delivered_table='vendas'
            )
            
            commit_checkpoint(
                checkpoint_name=checkpoint,
                nova_data_maxima=nova_data_maxima
            )
            
        except Exception as e:
            print(f"❌ Erro durante a inserção. O checkpoint NÃO foi atualizado. Erro: {e}")
            raise e
    else:
        print("ℹ️ Nenhum dado novo encontrado para este lote.")
