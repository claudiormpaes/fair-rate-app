import pandas as pd
import requests
import sqlite3
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
URL_BASE = "https://www.anbima.com.br/informacoes/est-termo/CZ.asp"
DB_NAME = "meu_app.db"

def buscar_dados_anbima():
    print(f"🔄 Conectando à ANBIMA...")
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Lógica Inteligente de Data (Sempre D-1 útil)
    hoje = datetime.now()
    if hoje.weekday() == 0: # Se é Segunda, pega Sexta
        data_target = hoje - timedelta(days=3)
    elif hoje.weekday() == 6: # Se é Domingo, pega Sexta
        data_target = hoje - timedelta(days=2)
    else: # Terça a Sábado, pega D-1
        data_target = hoje - timedelta(days=1)
        
    data_str = data_target.strftime("%d/%m/%Y")
    
    # Parâmetros para BAIXAR O XML (escolha=2)
    payload = {
        'Dt_Ref': data_str,
        'escolha': '2', # 2 = Download
        'saida': 'xml', # Formato XML
        'idioma': 'PT'
    }
    
    print(f"⬇️ Solicitando XML para a data: {data_str}...")
    
    try:
        response = session.post(URL_BASE, data=payload, headers=headers, timeout=20)
        response.raise_for_status()
        response.encoding = 'iso-8859-1' # Encoding padrão da Anbima
        
        # Verifica se veio uma página de erro HTML em vez de XML
        if "<!DOCTYPE html" in response.text[:100] or "<html" in response.text[:100]:
            print("⚠️ O site retornou HTML em vez de XML (provavelmente dados indisponíveis para esta data).")
            return None, None
            
        return response.text, data_str
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def processar_xml(xml_content):
    print("⚙️ Processando estrutura do XML...")
    
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        print("❌ Conteúdo inválido (não é XML).")
        return pd.DataFrame()

    dados = []
    
    # NOVA LÓGICA: Procura a tag <VERTICES> direto
    # Exemplo do arquivo: <VERTICES Vertice='252' IPCA='8,8600' Prefixados='13,2712' ... />
    
    for item in root.findall(".//VERTICES"):
        attr = item.attrib
        
        try:
            # 1. Tratar Vértice (Dias)
            # Remove pontos de milhar (Ex: '1.008' -> '1008')
            dias_str = attr.get('Vertice', '').replace('.', '')
            dias = int(dias_str)
            
            # 2. Tratar Taxas (Pré e IPCA)
            # Pode vir vazio '' ou com virgula '13,27'
            pre_str = attr.get('Prefixados', '').replace(',', '.')
            ipca_str = attr.get('IPCA', '').replace(',', '.')
            
            # Se algum campo estiver vazio, pula esse vértice
            if not pre_str or not ipca_str:
                continue
                
            taxa_pre = float(pre_str)
            taxa_ipca = float(ipca_str)
            
            dados.append({
                'dias': dias,
                'taxa_pre': taxa_pre,
                'taxa_ipca': taxa_ipca
            })
            
        except ValueError:
            continue # Pula erros de conversão
            
    df = pd.DataFrame(dados)
    
    if not df.empty:
        print(f"✅ Sucesso! {len(df)} vértices extraídos.")
    else:
        print("❌ XML lido, mas nenhum dado útil encontrado.")
        
    return df

def interpolar_curvas(df_raw, data_ref):
    print("📐 Calculando interpolação (0 a 5000 dias)...")
    
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    
    if len(df_raw) < 5:
        print("❌ Poucos pontos para interpolar.")
        return pd.DataFrame()

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
    
    # Limpa dados anteriores da mesma data
    cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print("✅ Banco de dados atualizado com sucesso!")

if __name__ == "__main__":
    xml_content, data_ref = buscar_dados_anbima()
    
    if xml_content:
        df_raw = processar_xml(xml_content)
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            exit(1) # Erro no GitHub
    else:
        exit(1) # Erro no GitHub
