import pandas as pd
import requests
import sqlite3
import numpy as np
import re
from datetime import datetime
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
# Mudamos para a página HTML visual, que é mais estável que o XML
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
        # Força encoding para corrigir acentos (Vértice, Pré, etc)
        response.encoding = response.apparent_encoding 
        return response.text
    except Exception as e:
        print(f"❌ Erro ao baixar página: {e}")
        return None

def processar_html(html_content):
    print("⚙️ Lendo tabelas do HTML...")
    
    try:
        # 1. Tentar extrair a DATA da página usando Regex
        # Procura por algo como "Data de referência: 30/01/2026"
        match_data = re.search(r'(\d{2}/\d{2}/\d{4})', html_content)
        if match_data:
            data_ref = match_data.group(1)
            print(f"📅 Data encontrada no HTML: {data_ref}")
        else:
            # Se falhar, usa a data de hoje como fallback (perigoso, mas evita crash)
            data_ref = datetime.now().strftime("%d/%m/%Y")
            print(f"⚠️ Data não encontrada no texto. Usando data de hoje: {data_ref}")

        # 2. Ler as tabelas usando Pandas
        # O replace ajuda a padronizar os decimais brasileiros antes de ler
        html_limpo = html_content.replace('.', '').replace(',', '.')
        dfs = pd.read_html(html_limpo, header=0)
        
        df_dados = pd.DataFrame()
        
        # Procura qual das tabelas tem a coluna "Vértice"
        for df in dfs:
            # Normaliza nomes das colunas para minúsculo para facilitar a busca
            df.columns = [c.lower() for c in df.columns]
            
            # Verifica se é a tabela certa
            if 'vértice' in str(df.columns) or 'vertice' in str(df.columns):
                print("✅ Tabela de curvas encontrada!")
                
                # Renomear colunas para o padrão que usamos
                # O nome das colunas muda as vezes, vamos pegar pela posição
                # Geralmente: Col 0=Vertice, Col 1=Dias, Col 2=Pré, Col 3=IPCA
                
                # Filtra apenas linhas que são números (remove rodapés)
                df = df[pd.to_numeric(df.iloc[:, 1], errors='coerce').notnull()]
                
                df_dados = pd.DataFrame({
                    'dias': pd.to_numeric(df.iloc[:, 1]), # Dias Corridos
                    'taxa_pre': pd.to_numeric(df.iloc[:, 2]), # Taxa Pré
                    'taxa_ipca': pd.to_numeric(df.iloc[:, 3]) # Taxa IPCA
                })
                break
        
        if df_dados.empty:
            print("❌ Nenhuma tabela de dados válida encontrada.")
            return pd.DataFrame(), None
            
        return df_dados, data_ref

    except Exception as e:
        print(f"❌ Erro ao processar HTML: {e}")
        return pd.DataFrame(), None

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando interpolação (Scipy)...")
    
    # Remove linhas com NaN (erros de leitura)
    df_raw = df_raw.dropna()
    
    # Ordena por dias
    df_raw = df_raw.sort_values('dias')
    
    # Cria a interpolação
    try:
        cs_pre = CubicSpline(df_raw['dias'], df_raw['taxa_pre'])
        cs_ipca = CubicSpline(df_raw['dias'], df_raw['taxa_ipca'])
        
        # Gera dias de 1 a 5000
        dias_full = np.arange(1, 5001)
        
        df_final = pd.DataFrame({
            'dias_corridos': dias_full,
            'taxa_pre': cs_pre(dias_full),
            'taxa_ipca': cs_ipca(dias_full),
            'data_referencia': data_ref
        })
        
        # Calcula Implícita
        df_final['inflacao_implicita'] = (
            ((1 + df_final['taxa_pre']/100) / (1 + df_final['taxa_ipca']/100)) - 1
        ) * 100
        
        return df_final
    except Exception as e:
        print(f"❌ Erro matemático na interpolação: {e}")
        return pd.DataFrame()

def salvar_banco(df_final, data_ref):
    if df_final.empty:
        print("⚠️ DataFrame vazio. Nada a salvar.")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Cria a tabela se não existir (segurança)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS curvas_anbima (
            dias_corridos INTEGER,
            taxa_pre REAL,
            taxa_ipca REAL,
            inflacao_implicita REAL,
            data_referencia TEXT
        )
    ''')
    
    # Verifica se já tem dados dessa data
    cursor.execute("SELECT count(*) FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    existe = cursor.fetchone()[0]
    
    if existe > 0:
        print(f"🔄 Dados de {data_ref} já existem. Substituindo...")
        cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    else:
        print(f"✨ Inserindo novos dados para {data_ref}...")
    
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print("✅ Banco de dados atualizado com sucesso!")

# --- EXECUÇÃO ---
if __name__ == "__main__":
    html = buscar_dados_anbima()
    if html:
        df_raw, data_ref = processar_html(html)
        if not df_raw.empty and data_ref:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            print("❌ Falha no processamento dos dados brutos.")
            # Força erro para o GitHub Actions ficar vermelho e avisar
            exit(1) 
    else:
        print("❌ Falha no download.")
        exit(1)
