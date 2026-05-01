import psycopg2
import json
import random
from faker import Faker
from datetime import datetime

fake = Faker('pt_BR')

# Conexão com o servidor Bronze (rodando dentro do Docker, a porta padrão é 5432 e o host é o nome do container)
# Se for rodar fora do Docker (na sua máquina), mude o host para 'localhost' e a porta para 5432
DB_CONFIG = {
    'dbname': 'db_bronze',
    'user': 'admin',
    'password': 'password',
    'host': 'pg_bronze', # Nome do container
    'port': '5432'
}

def setup_bronze_tables(cursor):
    """Cria as tabelas raw na camada Bronze preparadas para receber JSONB"""
    queries = [
        "CREATE TABLE IF NOT EXISTS db_bronze.raw.raw_users (payload JSON, data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS db_bronze.raw.raw_products (payload JSON, data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS db_bronze.raw.raw_sales (payload JSON, data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
    ]
    for q in queries:
        cursor.execute(q)

def generate_users(num_users):
    users = []
    for _ in range(num_users):
        user = {
            "user_id": fake.uuid4(),
            "nome": fake.name(),
            "email": fake.email(),
            "data_cadastro": fake.date_time_this_year().isoformat()
        }
        users.append(user)
    return users

def generate_products(num_products):
    products = []
    categorias = ['Eletrônicos', 'Roupas', 'Casa', 'Esportes']
    for _ in range(num_products):
        preco_custo = round(random.uniform(10.0, 500.0), 2)
        # Margem de lucro aleatória entre 20% e 100%
        preco_venda = round(preco_custo * random.uniform(1.2, 2.0), 2) 
        
        product = {
            "product_id": fake.uuid4(),
            "nome_produto": fake.word().capitalize() + " " + fake.word(),
            "categoria": random.choice(categorias),
            "preco_custo": preco_custo,
            "preco_venda": preco_venda
        }
        products.append(product)
    return products

def generate_sales(num_sales, users, products):
    sales = []
    for _ in range(num_sales):
        # 1 - À vista (Pix/Boleto), 2 - Cartão de Crédito, 3 - Cartão de Débito
        metodo_pagamento = random.choice([1, 2, 3])
        # 1 - Concluído, 2 - Pendente, 3 - Cancelado
        status_pedido = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0] 
        
        sale = {
            "sale_id": fake.uuid4(),
            "user_id": random.choice(users)['user_id'],
            "product_id": random.choice(products)['product_id'],
            "quantidade": random.randint(1, 5),
            "metodo_pagamento": metodo_pagamento,
            "status_pedido": status_pedido,
            "data_venda": fake.date_time_between(start_date='-30d', end_date='now').isoformat()
        }
        sales.append(sale)
    return sales

def insert_data(cursor, table, data_list):
    """Insere a lista de dicionários como JSON no banco de dados"""
    insert_query = f"INSERT INTO db_bronze.raw.{table} (payload) VALUES (%s)"
    for data in data_list:
        cursor.execute(insert_query, (json.dumps(data),))

def main(users:int,products:int,sales:int):
    print("Conectando ao PostgreSQL Bronze...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    setup_bronze_tables(cursor)

    print("Gerando dados fake...")
    users = generate_users(users)
    products = generate_products(products)
    sales = generate_sales(sales, users, products)

    print("Inserindo dados na camada Bronze...")
    insert_data(cursor, 'raw_users', users)
    insert_data(cursor, 'raw_products', products)
    insert_data(cursor, 'raw_sales', sales)

    conn.commit()
    cursor.close()
    conn.close()
    print("Ingestão concluída com sucesso!")

if __name__ == "__main__":
    main()