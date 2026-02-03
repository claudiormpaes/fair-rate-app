import pandas as pd
import requests
import sqlite3
import numpy as np
import re
from bs4 import BeautifulSoup
from datetime import datetime
from scipy.interpolate import CubicSpline
import io

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
        input_data = soup.find('input', {'name': 'Dt_Ref'})
        
        if input_data and input_data.get('value'):
            data_hoje = input_data['value']
            print(f"📅 Data encontrada no site: {data_hoje}")
        else:
            data_hoje = datetime.now().strftime("%d/%m/%Y")
            print(f"⚠️ Data não encontrada. Usando sistema: {data_hoje}")

        # PASSO 2: Simular o clique no botão "Consultar" (EM TELA)
        # escolha=1 significa "Em Tela"
        payload = {
            'Dt_Ref': data_hoje,
            'escolha': '1', 
            'idioma': 'PT',
            'saida': 'xls' # Padrão do form, mesmo sendo em tela
        }
        
        print(f"🖥️ Solicitando dados 'Em Tela' para {data_hoje}...")
        response_post = session.post(URL_BASE, data=payload, headers=headers, timeout=20)
        response_post.raise_for_status()
        
        # Corrige encoding para português
        response_post.encoding = response_post.apparent_encoding
        
        return response_post.text, data_hoje

    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def processar_html(html_content, data_ref):
    print("⚙️ Lendo tabelas do HTML...")
    
    try:
        # Lê todas as tabelas da página retornada
        # decimal=',' e thousands='.' fazem o Python entender números brasileiros (Ex: 1.000,50)
        dfs = pd.read_html(io.StringIO(html_content), decimal=',', thousands='.', header=0)
        
        df_dados = pd.DataFrame()
        
        for df in dfs:
            # Normaliza nomes das colunas
            cols = [str(c).lower().strip() for c in df.columns]
            
            # Procura a tabela certa (tem 'vértice' e 'pre')
            if any('vértice' in c for c in cols) or any('vertice' in c for c in cols):
                
                # Seleciona colunas pelo índice para garantir
                # Col 0: Vértice | Col 1: Dias | Col 2: Pré | Col 3: IPCA
                if len(df.columns) >= 4:
                    df = df.iloc[:, :4]
                    df.columns = ['vertice', 'dias', 'taxa_pre', 'taxa_ipca']
                    
                    # Garante que são números
                    df['dias'] = pd.to_numeric(df['dias'], errors='coerce')
                    df['taxa_pre'] = pd.to_numeric(df['taxa_pre'], errors='coerce')
                    df['taxa_ipca'] = pd.to_numeric(df['taxa_ipca'], errors='coerce')
                    
                    df_dados = df.dropna()
                    print(f"✅ Tabela encontrada com {len(df_dados)} linhas.")
                    break
        
        if df_dados.empty:
            print("❌ Nenhuma tabela de dados válida encontrada na resposta.")
            return pd.DataFrame()
            
        return df_dados

    except Exception as e:
        print(f"❌ Erro ao ler HTML: {e}")
        return pd.DataFrame()

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando interpolação (Scipy)...")
    
    # Ordena e remove duplicatas
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    
    # Filtra onde tem dados válidos para ambas as curvas
    df_clean = df_raw.dropna(subset=['taxa_pre', 'taxa_ipca'])
    
    if df_clean.empty:
        print("❌ Dados insuficientes para interpolação.")
        return pd.DataFrame()

    try:
        cs_pre = CubicSpline(df_clean['dias'], df_clean['taxa_pre'])
        cs_ipca = CubicSpline(df_clean['dias'], df_clean['taxa_ipca'])
        
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
        print(f"❌ Erro matemático na interpolação: {e}")
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
    print("✅ Sucesso Total! Banco atualizado.")

if __name__ == "__main__":
    html_content, data_ref = buscar_dados_anbima()
    
    if html_content and data_ref:
        df_raw = processar_html(html_content, data_ref)
        
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            print("❌ Falha: HTML retornado não continha a tabela esperada.")
            exit(1)
    else:
        print("❌ Falha: Não foi possível acessar o site.")
        exit(1)
