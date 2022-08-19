import glob
import pandas as pd
import requests

data_raw_path = "../data/raw/"
data_trusted_path = "../data/trusted/"
data_bank_splited_path = "../data/raw/original_bank_source/*."

encoding_format = 'Windows-1252'

####Consolidate Splited Banks Information Dataset
#Original Banks Information dataset are splited in csv files by year and period. This section aims to merge the splited files into a colosolidated csv of 2020 to 2021 period.
def merge_bank_info_csv():
    extension = 'csv'

    all_filenames = [i for i in glob.glob(data_bank_splited_path + '{}'.format(extension))]
    
    #Consolidate all csv files into one
    merge_banks_information_df = pd.concat([pd.read_csv(f, index_col=None, header=0, low_memory=False, delimiter=";", encoding=encoding_format) for f in all_filenames ])

    #Save bank info dataframe in raw bucket
    merge_banks_information_df.to_csv(data_raw_path + "bank_info.csv", sep=';', encoding=encoding_format, index=False)
    
    return(merge_banks_information_df)
    


#### Get Banking Fees through API
#This sections will perform url requests to an API using bank key (CNPJ) as variable parameter. 
#The result of each request is merged into consolidated json which will further trasformed into pandas dataset.
def get_bank_fees_api(bank_cnpj_df):
    
    cnpj_list = get_unique_cnpj_list(bank_cnpj_df)


    url = ("https://olinda.bcb.gov.br/olinda/servico/Informes_ListaTarifasPorInstituicaoFinanceira/versao/v1/odata/"
            "ListaTarifasPorInstituicaoFinanceira(PessoaFisicaOuJuridica=@PessoaFisicaOuJuridica,CNPJ=@CNPJ)?@PessoaFisicaOuJuridica='J'"
            "&@CNPJ='{0}'&$top=100&$format=json&$select=CodigoServico,Servico,Unidade,DataVigencia,ValorMaximo,TipoValor,Periodicidade")
    
    appended_data = []

    for i in cnpj_list:
        url_final = url.format(str(i))

        r = requests.get(url_final)
        result = r.json()
        bank_fees_df_list = pd.json_normalize(result['value'])
        
        #Insert bank id (CNPJ) in dataframe
        bank_fees_df_list.insert(0, 'CNPJ', str(i))
        appended_data.append(bank_fees_df_list)
        
    bank_fees_df = pd.concat(appended_data)

    #Save bank fees dataframe in raw bucket
    bank_fees_df.to_csv(data_raw_path + "bank_fees.csv", sep=';', encoding=encoding_format, index=False)

    return(bank_fees_df)


#The bank information dataset has a unique key for each bank (CNPJ). This key will further used to join with the bank fees dataset through API.
#This section will create a list of unique bank key (CNPJ) to get Banking Fees dataset. 
def get_unique_cnpj_list(bank_cnpj_df):
    list = bank_cnpj_df.replace(' ', None).dropna().drop_duplicates().tolist()
    return list


if __name__ == "__main__":
    merge_bank_info_csv()
    get_bank_fees_api()
