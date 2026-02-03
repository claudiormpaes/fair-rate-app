import pandas as pd
import requests
import sqlite3
import numpy as np
from datetime import datetime, timedelta
from scipy.interpolate import PchipInterpolator
import io

# --- CONFIGURAÇÕES ---
URL_DIRETA = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"
DB_NAME = "meu_app.db"

def buscar_dados_txt():
    print(f"🔄 Baixando arquivo direto: {URL_DIRETA}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(URL_DIRETA, headers=headers, timeout=30)
        response.raise_for_status()
        # CP1252 é o padrão Windows BR (melhor que latin-1 para Anbima)
        response.encoding = 'cp1252' 
        return response.text
    except Exception as e:
        print(f"❌ Erro no download: {e}")
        return None

def calcular_d1():
    # Calcula o dia útil anterior
    hoje = datetime.now()
    if hoje.weekday() == 0: # Segunda -> Sexta
        d1 = hoje - timedelta(days=3)
    elif hoje.weekday() == 6: # Domingo -> Sexta
        d1 = hoje - timedelta(days=2)
    else: # Terça a Sábado -> Ontem
        d1 = hoje - timedelta(days=1)
    return d1.strftime("%d/%m/%Y")

def processar_texto(conteudo_txt):
    print("⚙️ Processando arquivo de texto...")
    
    linhas = conteudo_txt.split('\n')
    
    # DEBUG: Mostra a primeira linha para conferirmos o formato
    if len(linhas) > 0:
        print(f"📄 Cabeçalho do arquivo: {linhas[0].strip()}")
    
    dados = []
    lendo_secao = False
    
    for linha in linhas:
        linha = linha.strip()
        
        # Procura seção de inflação
        if "ETTJ Inflação Implicita" in linha or "ETTJ Inflação Implícita" in linha:
            lendo_secao = True
            continue
            
        if lendo_secao:
            if "Vertices" in linha or "Vértice" in linha: continue
            if not linha or "PREFIXADOS" in linha or "Erro" in linha: break
            
            # Processa: 252;6,50;10,50;4,00
            if ';' in linha:
                partes = linha.split(';')
                try:
                    dias = int(partes[0].replace('.', ''))
                    ipca = float(partes[1].replace(',', '.'))
                    pre = float(partes[2].replace(',', '.'))
                    
                    dados.append({'dias': dias, 'taxa_pre': pre, 'taxa_ipca': ipca})
                except (ValueError, IndexError):
                    continue

    if not dados:
        print("❌ Não foi possível ler os dados.")
        return pd.DataFrame(), None

    df = pd.DataFrame(dados)
    
    # LÓGICA DE DATA (Corrigida)
    # Tenta ler do arquivo. Se falhar, usa D-1 calculado.
    data_ref = None
    try:
        # Tenta pegar "02/02/2026" da primeira linha
        # Ex: "Curva Zero - 02/02/2026"
        cabeçalho = linhas[0]
        if '-' in cabeçalho:
            possivel_data = cabeçalho.split('-')[-1].strip()
            datetime.strptime(possivel_data, "%d/%m/%Y") # Testa se é data válida
            data_ref = possivel_data
            print(f"📅 Data extraída do arquivo: {data_ref}")
    except:
        pass
        
    if not data_ref:
        data_ref = calcular_d1()
        print(f"⚠️ Data não encontrada no cabeçalho. Usando cálculo D-1: {data_ref}")
        
    return df, data_ref

def interpolar_curvas(df_raw, data_ref):
    print("📐 Interpolando curvas (PchipInterpolator)...")
    
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    df_clean = df_raw[(df_raw['taxa_pre'] > 0) & (df_raw['taxa_ipca'] > 0)]
    
    if len(df_clean) < 5:
        print("❌ Poucos dados.")
        return pd.DataFrame()

    try:
        pchip_pre = PchipInterpolator(df_clean['dias'], df_clean['taxa_pre'])
        pchip_ipca = PchipInterpolator(df_clean['dias'], df_clean['taxa_ipca'])
        
        dias_full = np.arange(1, 5001)
        
        df_final = pd.DataFrame({
            'dias_corridos': dias_full,
            'taxa_pre': pchip_pre(dias_full),
            'taxa_ipca': pchip_ipca(dias_full),
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
    
    # Remove se já existir, para garantir que não duplica
    cursor.execute("DELETE FROM curvas_anbima WHERE data_referencia = ?", (data_ref,))
    df_final.to_sql('curvas_anbima', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print(f"✅ Banco atualizado com referência: {data_ref}")

if __name__ == "__main__":
    txt = buscar_dados_txt()
    if txt:
        df, data = processar_texto(txt)
        if not df.empty:
            df_final = interpolar_curvas(df, data)
            salvar_banco(df_final, data)
        else:
            exit(1)
    else:
        exit(1)
