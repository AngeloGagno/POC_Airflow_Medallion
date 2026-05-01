from airflow.models import Variable

def get_checkpoint(checkpoint_name: str, default_date: str = '1900-01-01 00:00:00') -> str:
    """Busca a última marca d'água salva no Airflow."""
    return Variable.get(checkpoint_name, default_var=default_date)

def commit_checkpoint(checkpoint_name: str, nova_data_maxima):
    """Salva o novo estado no banco do Airflow de forma permanente."""
    if nova_data_maxima is not None and str(nova_data_maxima) != 'NaT':
        Variable.set(checkpoint_name, str(nova_data_maxima))
        print(f"✅ Checkpoint '{checkpoint_name}' atualizado com sucesso para: {nova_data_maxima}")
        return True
    return False