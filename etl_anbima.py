import pandas as pd
import requests
import sqlite3
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
URL_ANBIMA = "https://www.anbima.com.br/informacoes/est-termo/cz.xml"
DB_NAME = "meu_app.db"

def buscar_dados_anbima():
    print(f"🔄 Conectando à ANBIMA: {URL_ANBIMA}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    try:
        response = requests.get(URL_ANBIMA, headers=headers, timeout=10)
        response.raise_for_status() # Garante que não deu erro 404/500
        
        # O encoding da ANBIMA as vezes é latin-1, forçamos para evitar erro de acento
        response.encoding = 'iso-8859-1' 
        
        return response.content
    except Exception as e:
        print(f"❌ Erro ao baixar dados: {e}")
        return None

def processar_xml(xml_content):
    print("⚙️ Processando XML...")
    root = ET.fromstring(xml_content)
    
    # Pegar a data de referência do arquivo
    data_ref = root.find(".//data").text
    print(f"📅 Data encontrada no XML: {data_ref}")
    
    dados = []
    
    # Iterar sobre as curvas (Prefixado e IPCA)
    for grupo in root.findall(".//grupo"):
        indice = grupo.attrib['indice'] # Ex: PRE, IPCA, TR
        
        if indice in ['PRE', 'IPCA']:
            for vertice in grupo.findall("vertice"):
                dias = int(vertice.attrib['dias'])
                taxa = float(vertice.attrib['taxa'].replace(',', '.'))
                
                dados.append({
                    'indice': indice,
                    'dias': dias,
                    'taxa': taxa
                })
    
    df = pd.DataFrame(dados)
    return df, data_ref

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando interpolação (Scipy)...")
    
    # Separar as curvas
    df_pre = df_raw[df_raw['indice'] == 'PRE'].sort_values('dias')
    df_ipca = df_raw[df_raw['indice'] == 'IPCA'].sort_values('dias')
    
    # Criar funções de interpolação (Cubic Spline)
    # Isso cria uma curva suave entre os pontos que a ANBIMA deu
    cs_pre = CubicSpline(df_pre['dias'], df_pre['taxa'])
    cs_ipca = CubicSpline(df_ipca['dias'], df_ipca['taxa'])
    
    # Criar um range de dias úteis completo (do dia 1 até o dia 5000)
    dias_full = np.arange(1, 5001)
    
    # Calcular as taxas para todos esses dias
    taxas_pre = cs_pre(dias_full)
    taxas_ipca = cs_ipca(dias_full)
    
    # Montar o DataFrame final
    df_final = pd.DataFrame({
        'dias_corridos': dias_full,
        'taxa_pre': taxas_pre,
        'taxa_ipca': taxas_ipca
    })
    
    # Calcular Inflação Implícita (Fisher)
    # (1 + Pre) = (1 + Real) * (1 + Implicita)
    df_final['inflacao_implicita'] = (
        ((1 + df_final['taxa_pre']/100) / (1 + df_final['taxa_ipca']/100)) - 1
    ) * 100
    
    df_final['data_referencia'] = data_ref
    
    return df_final

def salvar_banco(df_final, data_ref):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Verifica se essa data já existe para não duplicar
    cursor.execute("SELECT count(*) FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    existe = cursor.fetchone()[0]
    
    if existe > 0:
        print(f"⚠️ Dados de {data_ref} já existem no banco. Atualizando...")
        cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.close()
    print("✅ Dados salvos com sucesso no SQLite!")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    content = buscar_dados_anbima()
    
    if content:
        df_raw, data_ref = processar_xml(content)
        
        if not df_raw.empty:
            df_interpolado = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_interpolado, data_ref)
        else:
            print("❌ O XML estava vazio ou com formato inesperado.")
    else:
        print("❌ Falha na conexão.")
