import pandas as pd
import requests
import sqlite3
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
URL_FORM = "https://www.anbima.com.br/pt_br/informar/curvas-de-juros-fechamento.htm" # Página Visível
URL_ACTION = "https://www.anbima.com.br/informacoes/est-termo/CZ.asp" # Backend
DB_NAME = "meu_app.db"

def buscar_xml_anbima():
    print(f"🔄 Iniciando sessão com ANBIMA...")
    
    # 1. Calcular a data D-1 (Ontem útil)
    hoje = datetime.now()
    if hoje.weekday() == 0: # Segunda -> Sexta
        data_target = hoje - timedelta(days=3)
    elif hoje.weekday() == 6: # Domingo -> Sexta
        data_target = hoje - timedelta(days=2)
    else: # Terça a Sábado -> Ontem
        data_target = hoje - timedelta(days=1)
        
    data_str = data_target.strftime("%d/%m/%Y")
    
    # 2. Criar Sessão (O Segredo!)
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': URL_FORM,
        'Origin': 'https://www.anbima.com.br'
    }
    
    try:
        # PASSO A: Acessar a página principal para pegar os Cookies
        print("🍪 Obtendo cookies de sessão...")
        session.get(URL_FORM, headers=headers, timeout=15)
        
        # PASSO B: Pedir o Download do XML
        payload = {
            'escolha': '2',      # Download
            'saida': 'xml',      # XML
            'idioma': 'PT',
            'Dt_Ref': data_str
        }
        
        print(f"⬇️ Baixando XML para: {data_str}...")
        response = session.post(URL_ACTION, data=payload, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = 'iso-8859-1'
        
        # Validação: Se não tiver a tag <CURVAZERO>, não é o arquivo certo
        if "<CURVAZERO>" not in response.text and "<curvazero>" not in response.text:
            print("⚠️ O site retornou algo que não é o XML esperado.")
            # Debug: mostra o começo do erro para sabermos o que é
            print(f"Inicio do conteúdo: {response.text[:200]}") 
            return None, None
            
        return response.text, data_str

    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def processar_xml(xml_content):
    print("⚙️ Lendo estrutura do XML...")
    
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        print("❌ XML mal formado.")
        return pd.DataFrame()

    dados = []
    
    # Busca todas as tags VERTICES (maiúsculo, conforme seu arquivo)
    # Estrutura: <VERTICES Vertice='252' IPCA='8,8600' Prefixados='13,2712' ... />
    elementos = root.findall(".//VERTICES")
    
    print(f"🔎 Encontrados {len(elementos)} pontos de curva.")

    for item in elementos:
        attr = item.attrib
        try:
            # 1. Dias (Vertice) - remover ponto (1.008 -> 1008)
            dias_str = attr.get('Vertice', '').replace('.', '')
            if not dias_str: continue
            dias = int(dias_str)
            
            # 2. Taxas - trocar vírgula por ponto
            pre_str = attr.get('Prefixados', '').replace(',', '.')
            ipca_str = attr.get('IPCA', '').replace(',', '.')
            
            # Se faltar taxa, pula
            if not pre_str or not ipca_str:
                continue
                
            dados.append({
                'dias': dias,
                'taxa_pre': float(pre_str),
                'taxa_ipca': float(ipca_str)
            })
        except ValueError:
            continue
            
    df = pd.DataFrame(dados)
    return df

def interpolar_curvas(df_raw, data_ref):
    print("📐 Gerando curva (Scipy CubicSpline)...")
    
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    
    # Limpeza básica
    df_clean = df_raw[(df_raw['taxa_pre'] > 0) & (df_raw['taxa_ipca'] > 0)]
    
    if len(df_clean) < 5:
        print("❌ Poucos dados para interpolar.")
        return pd.DataFrame()

    try:
        cs_pre = CubicSpline(df_clean['dias'], df_clean['taxa_pre'])
        cs_ipca = CubicSpline(df_clean['dias'], df_clean['taxa_ipca'])
        
        dias_full = np.arange(1, 5001) # Projeta até 5000 dias úteis
        
        df_final = pd.DataFrame({
            'dias_corridos': dias_full,
            'taxa_pre': cs_pre(dias_full),
            'taxa_ipca': cs_ipca(dias_full),
            'data_referencia': data_ref
        })
        
        # Inflação Implícita
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
    
    # Remove duplicatas da data
    cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print(f"✅ Sucesso! Dados de {data_ref} salvos no banco.")

if __name__ == "__main__":
    xml_content, data_ref = buscar_xml_anbima()
    
    if xml_content:
        df_raw = processar_xml(xml_content)
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            print("❌ XML válido, mas sem dados de vértices.")
            exit(1)
    else:
        exit(1)
