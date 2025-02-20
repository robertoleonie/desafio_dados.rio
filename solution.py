import pandas as pd
import numpy as np
from datetime import datetime

# Padronizar valores booleanos
def standardize_booleans(df, boolean_columns):
    for col in boolean_columns:
        # Converter para string, normalizar e mapear para booleanos
        df[col] = (
            df[col]
            .astype(str)
            .str.lower()
            .map({'0': False, '1': True, 'false': False, 'true': True})
            .astype(bool)  # Garantir que o tipo seja boolean
        )
    return df

# Identificar valores ausentes
def check_missing_values(df):
    missing_info = df.isnull().sum()
    missing_info = missing_info[missing_info > 0]
    return missing_info

# Padronizar categorias (remover espaços extras, normalizar case)
def standardize_categories(df, categorical_columns):
    for col in categorical_columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
    return df

# Verificar datas e identificar inconsistências
def check_dates(df, date_columns):
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].apply(lambda x: np.nan if (x is not pd.NaT and x > datetime.now()) else x)
    return df

# a. Remove valores inválidos da coluna raca_cor
def clean_raca_cor(df):
    valid_racas = ['amarela', 'branca', 'indígena', 'preta', 'parda']
    df['raca_cor'] = df['raca_cor'].apply(lambda x: x if x in valid_racas else np.nan)
    return df

# b. Remove valores inválidos da coluna religiao
def clean_religiao(df):
    df['religiao'] = df['religiao'].apply(lambda x: x if x != "10 eap 01" and isinstance(x, str) and "religião" not in x.lower() else np.nan)
    return df

# c. Remove valores inválidos da coluna renda_familiar
def clean_renda_familiar(df):
    df['renda_familiar'] = df['renda_familiar'].apply(lambda x: x if isinstance(x, str) and "salário(s) mínimo(s)" in x.lower() else np.nan)
    return df

# d. Remove valores distintos que representam a mesma coisa em uma coluna
def clean_duplicates(df, column):
    df[column] = df[column].str.normalize('NFKC')  # Normaliza caracteres Unicode
    df[column] = df[column].str.strip().str.lower()  # Remove espaços e padroniza case
    return df

# e. Remove "Não", "Sim" e categorias de sexualidade da coluna identidade_genero
def clean_identidade_genero(df):
    invalid_values = ['não', 'sim', 'homossexual', 'heterossexual', 'bissexual']
    df['identidade_genero'] = df['identidade_genero'].apply(lambda x: x if x not in invalid_values else np.nan)
    return df

# f. Aplica o processo análogo em d para as colunas meios_comunicacao e em_caso_doenca_procura
def clean_meios_comunicacao(df):
    df = clean_duplicates(df, 'meios_comunicacao')
    return df

def clean_em_caso_doenca_procura(df):
    df = clean_duplicates(df, 'em_caso_doenca_procura')
    return df

# g. Remove outliers que aparecem apenas uma vez (valores únicos)
def remove_unique_outliers(df, column):
    value_counts = df[column].value_counts()
    unique_values = value_counts[value_counts == 1].index
    df[column] = df[column].apply(lambda x: np.nan if x in unique_values else x)
    return df

# h. Remove outliers de altura e peso
def remove_height_weight_outliers(df):
    # Altura: Consideramos valores entre 0.5m e 2.5m como válidos
    df['altura'] = df['altura'].apply(lambda x: x if 0.5 <= x <= 2.5 else np.nan)
    
    # Peso: Consideramos valores entre 2kg e 300kg como válidos
    df['peso'] = df['peso'].apply(lambda x: x if 2 <= x <= 300 else np.nan)
    
    return df

# i. Corrige valores com duas casas antes da vírgula nas colunas pressao_sistolica e pressao_diastolica
def correct_pressure_values(df):
    for col in ['pressao_sistolica', 'pressao_diastolica']:
        df[col] = df[col].apply(lambda x: float(x) * 10 if isinstance(x, str) and ',' in x and len(x.split(',')[0]) == 2 else float(x))
    return df

# Função principal para processar o CSV
def process_csv(input_file, output_file):
    df = pd.read_csv(input_file)
    
    boolean_columns = ['obito', 'luz_eletrica', 'em_situacao_de_rua', 'frequenta_escola', 'possui_plano_saude',
                       'vulnerabilidade_social', 'familia_beneficiaria_auxilio_brasil',
                       'crianca_matriculada_creche_pre_escola']
    categorical_columns = ['identidade_genero', 'orientacao_sexual', 'bairro', 'raca_cor', 'ocupacao', 'religiao']
    date_columns = ['data_cadastro', 'data_nascimento', 'data_atualizacao_cadastro', 'updated_at']
    numeric_columns = ['altura', 'peso', 'pressao_sistolica', 'pressao_diastolica', 'n_atendimentos_atencao_primaria', 'n_atendimentos_hospital']
    
    # Padronizar valores booleanos
    df = standardize_booleans(df, boolean_columns)
    
    # Padronizar categorias
    df = standardize_categories(df, categorical_columns)
    
    # Verificar e corrigir datas
    df = check_dates(df, date_columns)
    
    # Limpeza específica das colunas
    df = clean_raca_cor(df)
    df = clean_religiao(df)
    df = clean_renda_familiar(df)
    df = clean_identidade_genero(df)
    df = clean_meios_comunicacao(df)
    df = clean_em_caso_doenca_procura(df)
    
    # Remover outliers
    df = remove_unique_outliers(df, 'meios_transporte')  # Exemplo para a coluna meios_transporte
    df = remove_height_weight_outliers(df)
    
    # Corrigir valores de pressão arterial
    df = correct_pressure_values(df)
    
    # Verificar valores ausentes restantes
    missing_info = check_missing_values(df)
    
    # Salvar o arquivo tratado
    df.to_csv(output_file, index=False)
    
    print("Arquivo tratado salvo em:", output_file)
    print("Valores ausentes restantes:", missing_info)

# Exemplo de uso
process_csv("dados_ficha_a_desafio.csv", "dados_ficha_a_tratado.csv")
