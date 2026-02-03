import pandas as pd
import requests
import sqlite3
import numpy as np
import io
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
URL_FORM = "https://www.anbima.com.br/pt_br/informar/curvas-de-juros-fechamento.htm"
URL_ACTION = "https://www.anbima.com.br/informacoes/est-termo/CZ.asp"
DB_NAME = "meu_app.db"

def buscar_dados_tela():
    print(f"🔄 Iniciando conexão (Modo Em Tela)...")
    
    # 1. Calcular Data (D-1 Útil)
    hoje = datetime.now()
    if hoje.weekday() == 0: # Segunda -> Sexta
        data_target = hoje - timedelta(days=3)
    elif hoje.weekday() == 6: # Domingo -> Sexta
        data_target = hoje - timedelta(days=2)
    else: # Terça a Sábado -> Ontem
        data_target = hoje - timedelta(days=1)
        
    data_str = data_target.strftime("%d/%m/%Y")
    
    # 2. Sessão com Headers de Navegador
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': URL_FORM,
        'Origin': 'https://www.anbima.com.br'
    }
    
    try:
        # Passo A: Acessar Home para pegar Cookies
        session.get(URL_FORM, headers=headers, timeout=10)
        
        # Passo B: POST pedindo "Em Tela" (escolha=1)
        payload = {
            'escolha': '1',      # 1 = EM TELA (HTML)
            'saida': 'xml',      # (Ignorado no modo tela, mas mantemos)
            'idioma': 'PT',
            'Dt_Ref': data_str
        }
        
        print(f"🖥️ Solicitando tabela para: {data_str}...")
        response = session.post(URL_ACTION, data=payload, headers=headers, timeout=20)
        response.raise_for_status()
        response.encoding = 'iso-8859-1' # Importante para acentos
        
        return response.text, data_str

    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def processar_html(html_content):
    print("⚙️ Lendo tabela HTML...")
    
    try:
        # Usa Pandas para ler as tabelas da página
        # decimal=',' e thousands='.' ajustam o padrão BR (1.000,00)
        dfs = pd.read_html(io.StringIO(html_content), decimal=',', thousands='.', header=0)
        
        df_dados = pd.DataFrame()
        
        for df in dfs:
            # Normaliza colunas
            cols = [str(c).lower().strip() for c in df.columns]
            
            # A tabela certa tem "Vértice" e "Pré" (ou Prefixados)
            if any('vértice' in c or 'vertice' in c for c in cols):
                
                # Seleciona as colunas fixas:
                # 0: Vértice | 1: Dias | 2: Pré | 3: IPCA
                # (Às vezes o site inverte Vértice/Dias, vamos garantir pelo nome se der, ou posição)
                
                # Pega as 4 primeiras colunas
                if len(df.columns) >= 4:
                    df = df.iloc[:, :4]
                    df.columns = ['vertice', 'dias', 'pre', 'ipca']
                    
                    # Limpeza forçada de números
                    df['dias'] = pd.to_numeric(df['dias'], errors='coerce')
                    df['pre'] = pd.to_numeric(df['pre'], errors='coerce')
                    df['ipca'] = pd.to_numeric(df['ipca'], errors='coerce')
                    
                    df_dados = df.dropna()
                    
                    # Renomeia para o nosso padrão
                    df_dados = df_dados.rename(columns={'pre': 'taxa_pre', 'ipca': 'taxa_ipca'})
                    print(f"✅ Tabela encontrada: {len(df_dados)} linhas.")
                    break
        
        if df_dados.empty:
            print("❌ Nenhuma tabela válida encontrada. Site pode ter retornado 'Não há dados'.")
            return pd.DataFrame()
            
        return df_dados

    except Exception as e:
        print(f"❌ Erro ao processar HTML: {e}")
        # Debug: imprime um pedaço do HTML para ver o erro
        print(f"HTML parcial: {html_content[:200]}")
        return pd.DataFrame()

def interpolar_curvas(df_raw, data_ref):
    print("📐 Gerando curva completa (Scipy)...")
    
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    df_clean = df_raw[(df_raw['taxa_pre'] > 0) & (df_raw['taxa_ipca'] > 0)]
    
    if len(df_clean) < 5:
        print("❌ Dados insuficientes.")
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
    
    cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print(f"✅ SUCESSO! Dados de {data_ref} gravados.")

if __name__ == "__main__":
    html, data_ref = buscar_dados_tela()
    
    if html:
        df_raw = processar_html(html)
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            print("❌ Falha: Tabela vazia.")
            exit(1)
    else:
        exit(1)
