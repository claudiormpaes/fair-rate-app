import pandas as pd
import requests
import sqlite3
import numpy as np
import io
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
URL_BASE = "https://www.anbima.com.br/informacoes/est-termo/CZ.asp"
DB_NAME = "meu_app.db"

def buscar_dados_anbima():
    print(f"🔄 Acessando ANBIMA: {URL_BASE}")
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # PASSO 1: Descobrir a data correta
        response_get = session.get(URL_BASE, headers=headers, timeout=15)
        response_get.raise_for_status()
        
        soup = BeautifulSoup(response_get.content, 'html.parser')
        input_data = soup.find('input', {'name': 'Dt_Ref'})
        
        data_consulta = None
        
        # Tenta pegar a data que já vem preenchida no site (é a mais confiável)
        if input_data and input_data.get('value'):
            data_consulta = input_data['value']
            print(f"📅 Data sugerida pelo site: {data_consulta}")
        
        # Se falhar, usa D-1 (Ontem) em vez de hoje
        if not data_consulta:
            ontem = datetime.now() - timedelta(days=1)
            # Se ontem foi domingo (6), volta para sexta (4)
            if ontem.weekday() == 6: ontem -= timedelta(days=2)
            # Se ontem foi sábado (5), volta para sexta (4)
            elif ontem.weekday() == 5: ontem -= timedelta(days=1)
            
            data_consulta = ontem.strftime("%d/%m/%Y")
            print(f"⚠️ Data não encontrada no HTML. Tentando dia útil anterior: {data_consulta}")

        # PASSO 2: Pedir a tabela "Em Tela"
        payload = {
            'Dt_Ref': data_consulta,
            'escolha': '1', # 1 = Em Tela
            'idioma': 'PT',
            'saida': 'xls'
        }
        
        print(f"🖥️ Consultando dados para {data_consulta}...")
        response_post = session.post(URL_BASE, data=payload, headers=headers, timeout=20)
        response_post.raise_for_status()
        response_post.encoding = response_post.apparent_encoding
        
        return response_post.text, data_consulta

    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def processar_html(html_content, data_ref):
    print("⚙️ Lendo tabelas...")
    
    try:
        # Lê tabelas convertendo padrão BR (1.000,00) para Python (1000.00)
        dfs = pd.read_html(io.StringIO(html_content), decimal=',', thousands='.', header=0)
        
        df_dados = pd.DataFrame()
        
        for df in dfs:
            # Limpa nomes das colunas (remove espaços e poe minusculo)
            cols = [str(c).lower().strip() for c in df.columns]
            
            # Debug: Mostra colunas encontradas para ajudar
            # print(f"Tabela encontrada com colunas: {cols}")

            # A tabela certa tem 'vertice' e ('prefixados' ou 'pre')
            tem_vertice = any('vértice' in c or 'vertice' in c for c in cols)
            tem_pre = any('prefixado' in c or 'pre' in c for c in cols)
            
            if tem_vertice and tem_pre:
                # Renomeia colunas pela POSIÇÃO (mais seguro que pelo nome)
                # Col 0: Vértice | Col 1: Dias | Col 2: Pré | Col 3: IPCA
                if len(df.columns) >= 4:
                    df = df.iloc[:, :4] # Pega só as 4 primeiras
                    df.columns = ['vertice', 'dias', 'taxa_pre', 'taxa_ipca']
                    
                    # Garante números
                    df['dias'] = pd.to_numeric(df['dias'], errors='coerce')
                    df['taxa_pre'] = pd.to_numeric(df['taxa_pre'], errors='coerce')
                    df['taxa_ipca'] = pd.to_numeric(df['taxa_ipca'], errors='coerce')
                    
                    df_dados = df.dropna()
                    print(f"✅ Tabela encontrada! {len(df_dados)} vértices importados.")
                    break
        
        if df_dados.empty:
            print("❌ Nenhuma tabela válida encontrada. O site pode ter retornado 'Não há dados'.")
            return pd.DataFrame()
            
        return df_dados

    except Exception as e:
        print(f"❌ Erro ao ler HTML: {e}")
        return pd.DataFrame()

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando curva completa (0 a 5000 dias)...")
    
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    
    # Remove zeros ou negativos que quebram log/divisão
    df_clean = df_raw[(df_raw['taxa_pre'] > 0) & (df_raw['taxa_ipca'] > 0)]
    
    if len(df_clean) < 5:
        print("❌ Poucos pontos para interpolar.")
        return pd.DataFrame()

    try:
        # CubicSpline para suavizar a curva
        cs_pre = CubicSpline(df_clean['dias'], df_clean['taxa_pre'])
        cs_ipca = CubicSpline(df_clean['dias'], df_clean['taxa_ipca'])
        
        dias_full = np.arange(1, 5001)
        
        df_final = pd.DataFrame({
            'dias_corridos': dias_full,
            'taxa_pre': cs_pre(dias_full),
            'taxa_ipca': cs_ipca(dias_full),
            'data_referencia': data_ref
        })
        
        # Fórmula de Fisher: (1+Pre) = (1+Real) * (1+Implicita)
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
    
    # Apaga dados velhos dessa mesma data se houver (para não duplicar)
    cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print("✅ Banco de dados salvo com sucesso!")

if __name__ == "__main__":
    html, data_ref = buscar_dados_anbima()
    
    if html and data_ref:
        df_raw = processar_html(html, data_ref)
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            print("❌ Falha: Dados vazios.")
            exit(1) # Força erro no GitHub
    else:
        exit(1)
