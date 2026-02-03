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
        'User-Agent': 'Mozilla/5.0'
    }
    try:
        response = requests.get(URL_ANBIMA, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'iso-8859-1'
        return response.content
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None

def processar_xml(xml_content):
    root = ET.fromstring(xml_content)
    data_ref = root.find(".//data").text
    dados = []
    for grupo in root.findall(".//grupo"):
        indice = grupo.attrib['indice']
        if indice in ['PRE', 'IPCA']:
            for vertice in grupo.findall("vertice"):
                dados.append({
                    'indice': indice,
                    'dias': int(vertice.attrib['dias']),
                    'taxa': float(vertice.attrib['taxa'].replace(',', '.'))
                })
    return pd.DataFrame(dados), data_ref

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando interpolação...")
    df_pre = df_raw[df_raw['indice'] == 'PRE'].sort_values('dias')
    df_ipca = df_raw[df_raw['indice'] == 'IPCA'].sort_values('dias')
    
    # Aqui usamos CubicSpline (que precisa do scipy)
    cs_pre = CubicSpline(df_pre['dias'], df_pre['taxa'])
    cs_ipca = CubicSpline(df_ipca['dias'], df_ipca['taxa'])
    
    dias_full = np.arange(1, 5001)
    df_final = pd.DataFrame({
        'dias_corridos': dias_full,
        'taxa_pre': cs_pre(dias_full),
        'taxa_ipca': cs_ipca(dias_full),
        'data_referencia': data_ref
    })
    
    df_final['inflacao_implicita'] = (((1 + df_final['taxa_pre']/100) / (1 + df_final['taxa_ipca']/100)) - 1) * 100
    return df_final

def salvar_banco(df_final, data_ref):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Limpa dados se já existirem para evitar duplicidade
    cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.close()

if __name__ == "__main__":
    content = buscar_dados_anbima()
    if content:
        df, data = processar_xml(content)
        df_final = interpolar_curvas(df, data)
        salvar_banco(df_final, data)
        print("✅ Sucesso!")
