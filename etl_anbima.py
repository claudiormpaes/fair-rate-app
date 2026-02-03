import pandas as pd
import requests
import sqlite3
import numpy as np
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
URL_BASE = "https://www.anbima.com.br/informacoes/est-termo/CZ.asp"
DB_NAME = "meu_app.db"

def buscar_dados_anbima():
    print(f"🔄 Acessando formulário da ANBIMA: {URL_BASE}")
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # PASSO 1: Entrar na página para pegar a DATA padrão
        response_get = session.get(URL_BASE, headers=headers, timeout=15)
        response_get.raise_for_status()
        
        soup = BeautifulSoup(response_get.content, 'html.parser')
        
        # Procura o campo de data no formulário (input name="Dt_Ref")
        input_data = soup.find('input', {'name': 'Dt_Ref'})
        
        if input_data and input_data.get('value'):
            data_hoje = input_data['value']
            print(f"📅 Data mais recente encontrada no site: {data_hoje}")
        else:
            # Se falhar, tenta data de hoje (pode dar erro se for feriado, mas tentamos)
            data_hoje = datetime.now().strftime("%d/%m/%Y")
            print(f"⚠️ Data não encontrada no input. Tentando data do sistema: {data_hoje}")

        # PASSO 2: Simular o clique no botão "Download XML"
        # Montamos o pacote de dados igual o navegador manda
        payload = {
            'Dt_Ref': data_hoje,
            'escolha': '2', # 2 = Download
            'saida': 'xml', # Formato XML
            'idioma': 'PT'
        }
        
        print(f"⬇️ Baixando XML para {data_hoje}...")
        response_post = session.post(URL_BASE, data=payload, headers=headers, timeout=20)
        response_post.raise_for_status()
        
        # O encoding correto geralmente é iso-8859-1 para Anbima
        response_post.encoding = 'iso-8859-1'
        
        return response_post.text, data_hoje

    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def processar_xml(xml_content, data_ref):
    print("⚙️ Processando arquivo XML...")
    
    try:
        # Tenta ler o XML
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        print("❌ O conteúdo baixado não é um XML válido. Pode ser um erro do site.")
        # Imprime os primeiros 100 caracteres para debug
        print(f"Conteúdo recebido: {xml_content[:100]}...") 
        return pd.DataFrame()

    dados = []
    
    # O XML da Anbima tem estrutura <grupo indice="PRE"> <vertice ... /> </grupo>
    for grupo in root.findall(".//grupo"):
        indice = grupo.attrib.get('indice') # Ex: PRE, IPCA
        
        if indice in ['PRE', 'IPCA']:
            for vertice in grupo.findall("vertice"):
                try:
                    dias = int(vertice.attrib['dias'])
                    # Troca virgula por ponto se necessário
                    taxa = float(vertice.attrib['taxa'].replace(',', '.'))
                    
                    dados.append({
                        'indice': indice,
                        'dias': dias,
                        'taxa': taxa
                    })
                except:
                    continue
    
    if not dados:
        print("❌ XML lido, mas nenhum dado de curva encontrado.")
        return pd.DataFrame()
        
    df = pd.DataFrame(dados)
    print(f"✅ Extraídos {len(df)} pontos da curva.")
    return df

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando interpolação (Scipy)...")
    
    df_pre = df_raw[df_raw['indice'] == 'PRE'].sort_values('dias')
    df_ipca = df_raw[df_raw['indice'] == 'IPCA'].sort_values('dias')
    
    if df_pre.empty or df_ipca.empty:
        print("❌ Faltam dados de PRE ou IPCA para interpolar.")
        return pd.DataFrame()

    try:
        cs_pre = CubicSpline(df_pre['dias'], df_pre['taxa'])
        cs_ipca = CubicSpline(df_ipca['dias'], df_ipca['taxa'])
        
        dias_full = np.arange(1, 5001)
        
        df_final = pd.DataFrame({
            'dias_corridos': dias_full,
            'taxa_pre': cs_pre(dias_full),
            'taxa_ipca': cs_ipca(dias_full),
            'data_referencia': data_ref
        })
        
        df_final['inflacao_implicita'] = (
            ((1 + df_final['taxa_pre']/100) / (1 + df_final['taxa_ipca']/100)) - 1
        ) * 100
        
        return df_final
    except Exception as e:
        print(f"❌ Erro matemático: {e}")
        return pd.DataFrame()

def salvar_banco(df_final, data_ref):
    if df_final.empty: return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS curvas_anbima (
            dias_corridos INTEGER,
            taxa_pre REAL,
            taxa_ipca REAL,
            inflacao_implicita REAL,
            data_referencia TEXT
        )
    ''')
    
    cursor.execute("SELECT count(*) FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    if cursor.fetchone()[0] > 0:
        print(f"🔄 Substituindo dados antigos de {data_ref}...")
        cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    else:
        print(f"✨ Inserindo novos dados de {data_ref}...")
    
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print("✅ Banco de dados atualizado com sucesso!")

if __name__ == "__main__":
    xml_content, data_ref = buscar_dados_anbima()
    
    if xml_content and data_ref:
        df_raw = processar_xml(xml_content, data_ref)
        
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            print("❌ Falha: Dados brutos vazios.")
            exit(1)
    else:
        print("❌ Falha: Não foi possível baixar o XML.")
        exit(1)
