import pandas as pd
import numpy as np
import unidecode
import re
from scipy import stats

data_raw_path = "../data/raw/"
data_trusted_path = "../data/trusted/"
data_bank_splited_path = "../data/raw/original_bank_source/*."

encoding_format = 'Windows-1252'

# Transform bank info dataset 'Informações Bancos'
def transform_bank_info():
    bancos_df = pd.read_csv(data_raw_path + 'bank_info.csv', sep=";",encoding= 'unicode_escape')
    
    bancos_df.drop("Unnamed: 14", axis=1, inplace=True)
    bancos_df = normaliza_df_header(bancos_df)

    def normalize_indice(value):
        if value == ' ':
            return None
        else:
            return float(value.replace('.','').replace(',','.'))

    def normalize_to_int(value):
        if value == ' ':
            return 0
        else:
            return int(value)

    bancos_df['indice'] = bancos_df.indice.apply(normalize_indice)
    bancos_df['quantidade_total_de_clientes_ccs_e_scr'] =  bancos_df.quantidade_total_de_clientes_ccs_e_scr.apply(normalize_to_int)
    bancos_df['quantidade_de_clientes_ccs'] =  bancos_df.quantidade_de_clientes_ccs.apply(normalize_to_int)
    bancos_df['quantidade_de_clientes_scr'] =  bancos_df.quantidade_de_clientes_scr.apply(normalize_to_int)
    
    bancos_df = normalize_lower_case_accent(bancos_df)

    #Save bank info dataframe in trusted bucket
    bancos_df.to_csv(data_trusted_path + "bank_info.csv", sep=';', encoding=encoding_format, index=False)
    
    return bancos_df


'''
Tranform Bank Fees Dataset (Tarifas Banco)
This function will perform data transformation by applying fields replacements and outliers exclusion with Z-score. 
The trasformed data will be saved at trusted bucked.
'''
def transform_bank_fees():
    #Read bank information dataset from raw layer
    lista_tarifas_df = pd.read_csv(data_raw_path + 'bank_fees.csv', sep=";",encoding= 'unicode_escape')

    #Convert columns data type
    convert_dict = {'CNPJ': int,
                'CodigoServico': int,
                'Servico': str,
                'Unidade': str,
                'DataVigencia': str,
                'ValorMaximo': float,
                'TipoValor': str,
                'Periodicidade': str
                }
    lista_tarifas_df = lista_tarifas_df.astype(convert_dict)

    #Normalizer string columns by removing accents and covert to lower case texts
    lista_tarifas_df = normalize_lower_case_accent(lista_tarifas_df)
    
    #Exclude outliers using Z-score with standard threshold of 3
    lista_tarifas_df = remove_outliers(lista_tarifas_df)
    lista_tarifas_df = normaliza_df_header(lista_tarifas_df)
    
    lista_tarifas_df['data_vigencia'] = pd.to_datetime(lista_tarifas_df.data_vigencia)
    
    #Save bank fees dataframe in trusted bucket
    lista_tarifas_df.to_csv(data_trusted_path + "bank_fees.csv", sep=';', encoding=encoding_format, index=False)
    
    return lista_tarifas_df



def normaliza_df_header(df):
    col_list = df.columns.tolist()
    new_cols = {}
    for col in col_list:
        new_cols[col] = unidecode.unidecode(camel_case_split(col))
    df.rename(columns=new_cols,inplace=True)
    return df


def camel_case_split(identifier):
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return '_'.join([m.group(0) for m in matches]).lower().replace("\x96","").replace('-', '').replace('  ','_').replace(' ', "_")


#Normalizer string columns by removing accents and covert to lower case texts   
def normalize_lower_case_accent(df):
    cols = df.select_dtypes(include=[object]).columns
    df[cols] = df[cols].apply(lambda x: x.str.normalize('NFKD').str.lower().str.encode('ascii', errors='ignore').str.decode('utf-8'))
    return df


#Exclude outliers using Z-score with standard threshold of 3
def remove_outliers(df):
    dataset_processed = df[(np.abs(stats.zscore(df['ValorMaximo'])) < 3)]
    return dataset_processed


if __name__ == "__main__":
    transform_bank_info()
    transform_bank_fees()



