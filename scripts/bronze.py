import pandas as pd
import json
import duckdb
from faker import Faker
import random
from scripts.gerar_dados_bronze import main

BRONZE_CON = "postgresql://admin:password@pg_bronze:5432/db_bronze"


def bronze_ecommerce():
    main(products=20,users=250000,sales=1000000)

