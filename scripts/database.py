import logging
import duckdb
from checkpoint import get_checkpoint,commit_checkpoint

class DatabaseFunctions:
    def __init__(self, db_con_string, database=None, schema=None, table=None):
        self.con = db_con_string
        self.database = database
        self.schema = schema
        self.table = table

    def _has_missing_args(self) -> bool:
        return any(x is None for x in [self.schema, self.database, self.table])

    def insert(self, sql_query: str, delivered_con_string: str, delivered_database: str, delivered_schema: str, delivered_table: str):
        if self._has_missing_args() or not delivered_database:
            logging.error('Por favor, forneça o database, schema e a tabela base da classe, além do banco de destino.')
            return 
            
        con_duckdb = duckdb.connect()
        try:
            con_duckdb.execute("INSTALL postgres; LOAD postgres;")
            
            con_duckdb.execute(f"ATTACH '{self.con}' AS \"{self.database}\" (TYPE POSTGRES);")
            con_duckdb.execute(f"ATTACH '{delivered_con_string}' AS \"{delivered_database}\" (TYPE POSTGRES);")
            con_duckdb.execute(f"USE \"{self.database}\";")
            
            full_insert_query = f"INSERT INTO \"{delivered_database}\".{delivered_schema}.{delivered_table} {sql_query}"
            
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
            con_duckdb.execute(f"ATTACH '{self.con}' AS \"{self.database}\" (TYPE POSTGRES);")      
            con_duckdb.execute(f"USE \"{self.database}\";")
            
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
            con_duckdb.execute(f"ATTACH '{self.con}' AS \"{self.database}\" (TYPE POSTGRES);")   
            con_duckdb.execute(f"USE \"{self.database}\";")
            
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
            con_duckdb.execute(f"ATTACH '{self.con}' AS \"{self.database}\" (TYPE POSTGRES);")   
            con_duckdb.execute(f"USE \"{self.database}\";")
            
            logging.info("Executando instrução DDL de criação...")
            con_duckdb.execute(sql_query)
            logging.info("Estrutura criada com sucesso no banco de dados!")
            
        except Exception as e:
            logging.error(f"Falha na execução do create: {e}")
            raise e
        finally:
            con_duckdb.close()
    def incremental_insert(
            self,
            target_con: str,
            target_db: str,
            target_schema: str,
            target_table: str,
            checkpoint_name: str,
            query_criacao_alvo: str,
            query_extracao_template: str,
            coluna_referencia_data: str
        ) -> None:
            """
            Executa um pipeline genérico de carga incremental entre duas camadas de dados.

            A função automatiza a criação da tabela de destino (se não existir), o resgate da marca 
            d'água (checkpoint) atual, a extração dos dados da origem baseada nessa data, a carga 
            no destino e a atualização segura do checkpoint apenas em caso de sucesso transacional.

            Args:
                target_con (str): String de conexão do banco de dados de destino.
                target_db (str): Nome do banco de dados de destino.
                target_schema (str): Nome do schema de destino (ex: 'public', 'silver').
                target_table (str): Nome da tabela de destino onde os dados serão inseridos.
                checkpoint_name (str): Nome único da variável no Airflow que guardará a última data processada.
                query_criacao_alvo (str): Query DDL completa (CREATE TABLE IF NOT EXISTS) para a tabela de destino.
                query_extracao_template (str): Query SQL de extração. DEVE conter a tag '{ultima_data}' 
                    no filtro WHERE para injeção dinâmica do limite temporal pelo Python.
                coluna_referencia_data (str, opcional): Nome da coluna na origem usada para calcular 
                    a data máxima do lote processado. O padrão é "dt_venda".

            Raises:
                Exception: Propaga qualquer erro ocorrido durante a inserção, garantindo que 
                    o fluxo seja interrompido e o checkpoint não seja atualizado indevidamente.
            """
            target_banco = DatabaseFunctions(
                db_con_string=target_con, 
                database=target_db, 
                schema=target_schema, 
                table=target_table
            )
            target_banco.create(query_criacao_alvo)

            ultima_data = get_checkpoint(checkpoint_name)
            query_extracao_formatada = query_extracao_template.format(ultima_data=ultima_data)

            query_max_date = f"SELECT MAX({coluna_referencia_data}) FROM ({query_extracao_formatada}) AS batch_limitado"
            df_max = self.select(sql_query=query_max_date, output_format='df')
            nova_data_maxima = df_max.iloc[0, 0]

            if nova_data_maxima is not None and str(nova_data_maxima) != 'NaT':
                try:
                    self.insert(
                        sql_query=query_extracao_formatada,
                        delivered_con_string=target_con,
                        delivered_schema=target_schema,
                        delivered_table=target_table
                    )
                    commit_checkpoint(
                        checkpoint_name=checkpoint_name,
                        nova_data_maxima=nova_data_maxima
                    )          
                except Exception as e:
                    raise e