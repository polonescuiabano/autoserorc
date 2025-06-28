import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Conectar ao MongoDB
client = MongoClient('mongodb://admin:senhaforte@serorc.serpra.com.br:27017/')
db = client['serorc']
insumos_collection = db['insumos']

# Data da nova cotação
data_cotacao = datetime(2025, 1, 1)

# Caminho do arquivo
excel_path = r"C:\Users\Dell\Downloads\202501\SINAPI_Referência_2025_01.xlsx"

abas = {
    'ICD': 'preco_desonerado',
    'ISD': 'preco_nao_desonerado'
}

for aba, tipo_preco in abas.items():
    print(f"Processando aba: {aba}")
    df = pd.read_excel(excel_path, sheet_name=aba, header=None)

    # Identificar colunas com "MT" na linha 10 (índice 9)
    linha_estado = df.iloc[9]
    colunas_mt = [i for i, val in linha_estado.items() if str(val).strip().upper() == 'MT']

    if not colunas_mt:
        print(f"Nenhuma coluna 'MT' encontrada na aba {aba}. Pulando.")
        continue

    # Processar a partir da linha 11 (índice 10)
    for i in range(4430, len(df)):
        row = df.iloc[i]

        codigo = row[1]
        nome = str(row[2]).strip()
        unidade = str(row[3]).strip()

        # Ignorar linhas sem código ou nome
        if pd.isna(codigo) or pd.isna(nome):
            continue

        preco_valido = None
        for col in colunas_mt:
            preco_raw = row[col]
            if pd.notna(preco_raw):
                try:
                    if isinstance(preco_raw, str):
                        preco_str = preco_raw.strip()
                    else:
                        preco_str = str(preco_raw)

                    preco_float = float(preco_str)
                    preco_valido = preco_float
                    break
                except (ValueError, TypeError):
                    continue

                except ValueError:
                    continue

        if preco_valido is None:
            continue  # Pular se não houver preço válido

        # Construir entrada de preço
        preco_cotacao_entry = {
            "preco_desonerado": None,
            "preco_nao_desonerado": None,
            "data_cotacao": data_cotacao
        }

        # Corrigir preenchimento dependendo da aba
        if aba == "ICD":
            preco_cotacao_entry["preco_desonerado"] = preco_valido
        elif aba == "ISD":
            preco_cotacao_entry["preco_nao_desonerado"] = preco_valido

        # Verificar se o insumo já existe (por código e nome)
        insumo_existente = insumos_collection.find_one({
            "codigo": codigo,
            "nome": nome
        })

        if insumo_existente:
            # Atualizar insumo com novo preço
            insumos_collection.update_one(
                {"_id": insumo_existente["_id"]},
                {"$push": {"precos_cotacao": preco_cotacao_entry}}
            )
        else:
            # Criar novo insumo
            novo_insumo = {
                "codigo": codigo,
                "nome": nome,
                "tipo": "SINAPI",
                "unidade_medida": unidade,
                "empresa": None,
                "precos_cotacao": [preco_cotacao_entry]
            }
            insumos_collection.insert_one(novo_insumo)

print("✅ Processamento finalizado com sucesso.")
client.close()
