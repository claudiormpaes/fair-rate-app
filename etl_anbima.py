import pandas as pd
import requests
import sqlite3
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from scipy.interpolate import CubicSpline

# --- CONFIGURAÇÕES ---
# URL do formulário (backend)
URL_POST = "https://www.anbima.com.br/informacoes/est-termo/CZ.asp"
# URL da página visual (para o Referer)
URL_REFERER = "https://www.anbima.com.br/pt_br/informar/curvas-de-juros-fechamento.htm"
DB_NAME = "meu_app.db"

def buscar_xml_anbima():
    print(f"🔄 Iniciando conexão com ANBIMA...")
    
    # 1. Calcular a Data Correta (Sempre D-1 útil)
    # Se hoje é 03/02, queremos 02/02. Se for segunda, queremos sexta.
    hoje = datetime.now()
    
    if hoje.weekday() == 0: # Segunda-feira -> Pega Sexta
        data_target = hoje - timedelta(days=3)
    elif hoje.weekday() == 6: # Domingo -> Pega Sexta
        data_target = hoje - timedelta(days=2)
    else: # Terça a Sábado -> Pega Ontem
        data_target = hoje - timedelta(days=1)
        
    data_str = data_target.strftime("%d/%m/%Y")
    
    # 2. Configurar Headers para parecer um navegador real
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': URL_REFERER, # <--- O SEGREDO ESTÁ AQUI
        'Origin': 'https://www.anbima.com.br',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    # 3. Payload (Simulando: Download -> XML -> Data -> Consultar)
    payload = {
        'escolha': '2',      # 2 = Download
        'saida': 'xml',      # Formato XML
        'idioma': 'PT',
        'Dt_Ref': data_str   # A data calculada (D-1)
    }
    
    print(f"⬇️ Baixando XML para data: {data_str}...")
    
    try:
        # Faz o POST direto (simulando o clique em Consultar)
        response = session.post(URL_POST, data=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Ajusta encoding
        response.encoding = 'iso-8859-1'
        
        # Verifica se deu certo ou se veio página de erro
        if "<!DOCTYPE html" in response.text[:200] or "<html" in response.text[:200]:
            print("⚠️ O site retornou HTML de erro. Provavelmente a data ainda não está disponível.")
            print(f"Conteúdo parcial: {response.text[:100]}...")
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
        print("❌ Arquivo inválido (não é XML).")
        return pd.DataFrame()

    dados = []
    
    # O XML que você mandou tem a tag <VERTICES ... /> dentro de <ETTJ>
    # Vamos buscar todas as tags VERTICES em qualquer lugar do arquivo
    elementos = root.findall(".//VERTICES")
    
    if not elementos:
        # Tenta minúsculo por garantia
        elementos = root.findall(".//vertices")
    
    print(f"🔎 Encontrados {len(elementos)} vértices no arquivo.")

    for item in elementos:
        attr = item.attrib
        
        try:
            # Estrutura do seu arquivo:
            # Vertice='252' IPCA='8,8600' Prefixados='13,2712'
            
            # 1. Dias (Vertice) - remover ponto de milhar (1.008 -> 1008)
            dias_raw = attr.get('Vertice') or attr.get('vertice')
            dias = int(dias_raw.replace('.', ''))
            
            # 2. Taxas - trocar vírgula por ponto
            pre_str = attr.get('Prefixados', '').replace(',', '.')
            ipca_str = attr.get('IPCA', '').replace(',', '.')
            
            # Se a taxa estiver vazia, pula
            if not pre_str or not ipca_str:
                continue
                
            dados.append({
                'dias': dias,
                'taxa_pre': float(pre_str),
                'taxa_ipca': float(ipca_str)
            })
            
        except (ValueError, AttributeError):
            continue
            
    df = pd.DataFrame(dados)
    return df

def interpolar_curvas(df_raw, data_ref):
    print("📐 Gerando curva completa (Scipy)...")
    
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    
    # Remove inconsistências
    df_clean = df_raw[(df_raw['taxa_pre'] > 0) & (df_raw['taxa_ipca'] > 0)]
    
    if len(df_clean) < 10:
        print("❌ Dados insuficientes para interpolar.")
        return pd.DataFrame()

    try:
        # Cria a função da curva
        cs_pre = CubicSpline(df_clean['dias'], df_clean['taxa_pre'])
        cs_ipca = CubicSpline(df_clean['dias'], df_clean['taxa_ipca'])
        
        # Gera dias de 1 a 5000 (aprox 20 anos)
        dias_full = np.arange(1, 5001)
        
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
    
    # Cria tabela se não existir
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS curvas_anbima (
            dias_corridos INTEGER,
            taxa_pre REAL,
            taxa_ipca REAL,
            inflacao_implicita REAL,
            data_referencia TEXT
        )
    ''')
    
    # Remove dados antigos dessa data para não duplicar
    cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    
    # Salva
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print("✅ Sucesso! Banco atualizado.")

if __name__ == "__main__":
    xml_content, data_ref = buscar_xml_anbima()
    
    if xml_content:
        df_raw = processar_xml(xml_content)
        
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            print("❌ XML baixado mas vazio de dados.")
            exit(1)
    else:
        # Se falhou o download, encerra com erro para o GitHub avisar
        exit(1)
