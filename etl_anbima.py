import pandas as pd
import requests
import sqlite3
import numpy as np
from datetime import datetime
from scipy.interpolate import PchipInterpolator # Usando a matemática do seu script
import io

# --- CONFIGURAÇÕES ---
# Essa é a URL do seu script local. Ela baixa o arquivo mais recente direto!
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
        
        # O arquivo geralmente vem em Latin-1 (padrão Brasil antigo)
        response.encoding = 'latin-1'
        
        return response.text
    except Exception as e:
        print(f"❌ Erro no download: {e}")
        return None

def processar_texto(conteudo_txt):
    print("⚙️ Processando arquivo de texto...")
    
    linhas = conteudo_txt.split('\n')
    dados = []
    
    # Variáveis para controlar a leitura (igual ao seu script)
    lendo_secao = False
    
    for linha in linhas:
        linha = linha.strip()
        
        # Procura o cabeçalho da seção de inflação (Lógica do seu script)
        if "ETTJ Inflação Implicita" in linha or "ETTJ Inflação Implícita" in linha:
            lendo_secao = True
            continue
            
        if lendo_secao:
            # Se a linha começar com "Vertices", é cabeçalho, pula
            if "Vertices" in linha or "Vértice" in linha:
                continue
                
            # Se a linha estiver vazia ou mudar de seção, para
            if not linha or "PREFIXADOS" in linha or "Erro" in linha:
                break
            
            # Processa a linha: 252;6,50;10,50;4,00
            if ';' in linha:
                partes = linha.split(';')
                try:
                    # Coluna 0: Dias | Col 1: IPCA | Col 2: Pré
                    dias = int(partes[0].replace('.', ''))
                    ipca = float(partes[1].replace(',', '.'))
                    pre = float(partes[2].replace(',', '.'))
                    
                    dados.append({
                        'dias': dias,
                        'taxa_pre': pre,
                        'taxa_ipca': ipca
                    })
                except (ValueError, IndexError):
                    continue

    if not dados:
        print("❌ Não foi possível ler os dados do texto.")
        return pd.DataFrame(), None

    df = pd.DataFrame(dados)
    
    # Tenta achar a data no arquivo (geralmente está na primeira linha)
    # Ex: "Curva Zero - 02/02/2026"
    try:
        data_ref = linhas[0].split('-')[-1].strip()
        # Valida se parece uma data
        datetime.strptime(data_ref, "%d/%m/%Y")
    except:
        # Se falhar, usa hoje como fallback
        data_ref = datetime.now().strftime("%d/%m/%Y")
        
    print(f"✅ Dados extraídos! Referência: {data_ref}")
    return df, data_ref

def interpolar_curvas(df_raw, data_ref):
    print("📐 Interpolando curvas (PchipInterpolator)...")
    
    df_raw = df_raw.sort_values('dias').drop_duplicates('dias')
    
    # Remove inconsistências
    df_clean = df_raw[(df_raw['taxa_pre'] > 0) & (df_raw['taxa_ipca'] > 0)]
    
    if len(df_clean) < 5:
        print("❌ Poucos dados para interpolar.")
        return pd.DataFrame()

    try:
        # Usando PchipInterpolator (igual ao seu script desktop)
        # Ele é melhor que CubicSpline para juros pois evita oscilações malucas
        pchip_pre = PchipInterpolator(df_clean['dias'], df_clean['taxa_pre'])
        pchip_ipca = PchipInterpolator(df_clean['dias'], df_clean['taxa_ipca'])
        
        dias_full = np.arange(1, 5001)
        
        df_final = pd.DataFrame({
            'dias_corridos': dias_full,
            'taxa_pre': pchip_pre(dias_full),
            'taxa_ipca': pchip_ipca(dias_full),
            'data_referencia': data_ref
        })
        
        # Cálculo da Implícita
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
    print(f"✅ Sucesso! Banco atualizado com dados de {data_ref}.")

if __name__ == "__main__":
    txt_content = buscar_dados_txt()
    
    if txt_content:
        df_raw, data_ref = processar_texto(txt_content)
        if not df_raw.empty:
            df_final = interpolar_curvas(df_raw, data_ref)
            salvar_banco(df_final, data_ref)
        else:
            exit(1)
    else:
        exit(1)
