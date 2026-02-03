import pandas as pd
import requests
import sqlite3
import numpy as np
import re
import io
from datetime import datetime
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
URL_ANBIMA = "https://www.anbima.com.br/informacoes/est-termo/CZ.asp"
DB_NAME = "meu_app.db"

def buscar_dados_anbima():
    print(f"🔄 Conectando à página da ANBIMA: {URL_ANBIMA}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(URL_ANBIMA, headers=headers, timeout=15)
        response.raise_for_status()
        # Ajuste de encoding para português
        response.encoding = response.apparent_encoding 
        return response.text
    except Exception as e:
        print(f"❌ Erro ao baixar página: {e}")
        return None

def processar_html(html_content):
    print("⚙️ Lendo tabelas do HTML...")
    
    try:
        # 1. Extrair Data (Regex robusto)
        match_data = re.search(r'(\d{2}/\d{2}/\d{4})', html_content)
        if match_data:
            data_ref = match_data.group(1)
            print(f"📅 Data encontrada: {data_ref}")
        else:
            data_ref = datetime.now().strftime("%d/%m/%Y")
            print(f"⚠️ Data não encontrada. Usando hoje: {data_ref}")

        # 2. Ler Tabelas (Sem quebrar o HTML)
        # Usamos io.StringIO para evitar o FutureWarning do Pandas
        dfs = pd.read_html(io.StringIO(html_content), header=0, decimal=',', thousands='.')
        
        df_dados = pd.DataFrame()
        
        for df in dfs:
            # Normaliza colunas para facilitar busca
            cols_lower = [str(c).lower().strip() for c in df.columns]
            
            # Procura tabela que tenha "Vértice" e "Pré"
            if any('vértice' in c for c in cols_lower) or any('vertice' in c for c in cols_lower):
                print("✅ Tabela de curvas encontrada!")
                
                # Às vezes o header vem sujo, vamos garantir nomes limpos
                # Padrão Anbima: Col 0=Vértice, Col 1=Dias, Col 2=Pré, Col 3=IPCA
                if len(df.columns) >= 4:
                    df = df.iloc[:, 0:4] # Pega só as 4 primeiras colunas
                    df.columns = ['vertice', 'dias', 'pre', 'ipca']
                    
                    # Limpeza de dados (Converter texto para número)
                    # Força conversão de erros para NaN e depois remove
                    df['dias'] = pd.to_numeric(df['dias'], errors='coerce')
                    df['pre'] = pd.to_numeric(df['pre'], errors='coerce')
                    df['ipca'] = pd.to_numeric(df['ipca'], errors='coerce')
                    
                    df_dados = df.dropna()
                    
                    # Renomeia para o padrão do nosso banco
                    df_dados = df_dados.rename(columns={
                        'pre': 'taxa_pre',
                        'ipca': 'taxa_ipca'
                    })
                    break
        
        if df_dados.empty:
            print("❌ Nenhuma tabela válida encontrada (verifique se o layout da Anbima mudou).")
            return pd.DataFrame(), None
            
        return df_dados, data_ref

    except Exception as e:
        print(f"❌ Erro crítico no processamento: {e}")
        return pd.DataFrame(), None

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando interpolação...")
    
    df_raw = df_raw.sort_values('dias')
    
    try:
        cs_pre = CubicSpline(df_raw['dias'], df_raw['taxa_pre'])
        cs_ipca = CubicSpline(df_raw['dias'], df_raw['taxa_ipca'])
        
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
        print(f"🔄 Atualizando dados de {data_ref}...")
        cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    else:
        print(f"✨ Inserindo dados de {data_ref}...")
    
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.close()
    print("✅ Banco salvo!")

if __name__ == "__main__":
    html = buscar_dados_anbima()
    if html:
        df, data = processar_html(html)
        if not df.empty:
            df_final = interpolar_curvas(df, data)
            salvar_banco(df_final, data)
        else:
            exit(1) # Força erro no GitHub se falhar
    else:
        exit(1)
