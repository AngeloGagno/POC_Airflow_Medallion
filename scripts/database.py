import logging
import duckdb

class DatabaseFunctions:
    def __init__(self, db_con_string, database=None, schema=None, table=None):
        self.con = db_con_string
        self.database = database
        self.schema = schema
        self.table = table

    def _has_missing_args(self) -> bool:
        return any(x is None for x in [self.schema, self.database, self.table])

    def insert(self, sql_query: str, delivered_con_string: str, delivered_schema: str, delivered_table: str):
        if self._has_missing_args():
            logging.error('Por favor, forneça o database, schema e a tabela base da classe.')
            return 
            
        con_duckdb = duckdb.connect()
        try:
            con_duckdb.execute("INSTALL postgres; LOAD postgres;")
            
            con_duckdb.execute(f"ATTACH '{self.con}' AS db_origem (TYPE POSTGRES);")
            con_duckdb.execute(f"ATTACH '{delivered_con_string}' AS db_destino (TYPE POSTGRES);")
            full_insert_query = f"INSERT INTO db_destino.{delivered_schema}.{delivered_table} {sql_query}"
            
            logging.info(f"Executando movimentação de dados: {full_insert_query}")
            con_duckdb.execute(full_insert_query)
            logging.info("Dados inseridos com sucesso!")
            
        except Exception as e:
            logging.error(f"Falha na execução do insert com DuckDB: {e}")
            raise e 
        finally:
            con_duckdb.close()

    def select(self, sql_query: str, output_format: str = 'df'):
        if self._has_missing_args():
            logging.error('Por favor, forneça o database, schema e a tabela base da classe.')
            return None

        con_duckdb = duckdb.connect()
        try:
            con_duckdb.execute("INSTALL postgres; LOAD postgres;")
            con_duckdb.execute(f"ATTACH '{self.con}' AS db_origem (TYPE POSTGRES);")      
            
            if output_format == 'df':
                return con_duckdb.execute(sql_query).df() 
            else:
                return con_duckdb.execute(sql_query).fetchall()
                
        except Exception as e:
            logging.error(f"Falha na execução do select: {e}")
            raise e
        finally:
            con_duckdb.close()
            
    def delete(self, sql_query: str):
        if self._has_missing_args():
            logging.error('Por favor, forneça o database, schema e a tabela base da classe.')
            return

        con_duckdb = duckdb.connect()
        try:
            con_duckdb.execute("INSTALL postgres; LOAD postgres;")
            con_duckdb.execute(f"ATTACH '{self.con}' AS db_origem (TYPE POSTGRES);")   
            
            logging.info("Executando delete...")
            con_duckdb.execute(sql_query)
            
        except Exception as e:
            logging.error(f"Falha na execução do delete: {e}")
            raise e
        finally:
            con_duckdb.close()

    def create(self, sql_query: str):
            if self._has_missing_args():
                logging.error('Por favor, forneça o database, schema e a tabela base da classe.')
                return

            con_duckdb = duckdb.connect()
            try:
                con_duckdb.execute("INSTALL postgres; LOAD postgres;")                
                con_duckdb.execute(f"ATTACH '{self.con}' AS db_alvo (TYPE POSTGRES);")   
                
                logging.info("Executando instrução DDL de criação...")
                con_duckdb.execute(sql_query)
                logging.info("Estrutura criada com sucesso no banco de dados!")
                
            except Exception as e:
                logging.error(f"Falha na execução do create: {e}")
                raise e
            finally:
                con_duckdb.close()