import pandas as pd
import sqlite3
import requests
import numpy as np
from scipy.interpolate import PchipInterpolator
from io import BytesIO
import datetime
import re

print("🚀 Iniciando ETL FAIR RATE (Motor: ANBIMA)...")

# --- 1. CONFIGURAÇÕES ---
URL_ANBIMA = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

def processar_dados_anbima():
    # 1. Download do Arquivo
    print("⏳ Baixando dados da ANBIMA...")
    try:
        response = requests.get(URL_ANBIMA)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Erro no download: {e}")
        return

    # 2. Ler o conteúdo
    conteudo = response.content.decode('latin-1')
    linhas = conteudo.split('\n')
    
    print("✅ Download concluído. Processando arquivo...")

    # --- NOVO: IDENTIFICAR A DATA DO ARQUIVO ---
    # A ANBIMA costuma colocar a data na primeira linha ou no nome do arquivo
    # Vamos tentar achar um padrão de data (DD/MM/AAAA) nas primeiras 5 linhas
    data_arquivo = datetime.datetime.now().strftime("%d/%m/%Y") # Valor padrão (hoje)
    
    padrao_data = r"(\d{2}/\d{2}/\d{4})" # Regex para procurar datas
    
    for i, linha in enumerate(linhas[:5]):
        match = re.search(padrao_data, linha)
        if match:
            data_arquivo = match.group(1)
            print(f"📅 Data de Referência encontrada no arquivo: {data_arquivo}")
            break
            
    # Caso não ache (fallback), usamos a data informada por você para hoje
    if "30/01/2026" in conteudo: # Verificação simples extra
         pass 

    # 3. Parser (Extração dos dados)
    ettj_dados = {
        'Vertices': [],
        'ETTJ_IPCA': [],
        'ETTJ_PREF': [],
        'Inflacao_Implicita': []
    }
    
    section = False
    
    for linha in linhas:
        linha = linha.strip()
        
        if "ETTJ Inflação Implicita" in linha or "ETTJ Inflação Implícita" in linha:
            section = True
            continue
            
        if section and "Vertices" in linha:
            continue 
            
        if section and (not linha or 'PREFIXADOS' in linha or 'Erro Título' in linha):
            break
            
        if section and ';' in linha:
            parts = linha.split(';')
            try:
                if len(parts) > 3:
                    v = int(parts[0].replace('.', '').strip())
                    ipca = float(parts[1].replace(',', '.').strip())
                    pre = float(parts[2].replace(',', '.').strip())
                    inf = float(parts[3].replace(',', '.').strip())
                    
                    ettj_dados['Vertices'].append(v)
                    ettj_dados['ETTJ_IPCA'].append(ipca)
                    ettj_dados['ETTJ_PREF'].append(pre)
                    ettj_dados['Inflacao_Implicita'].append(inf)
            except:
                continue

    df = pd.DataFrame(ettj_dados)
    
    if df.empty:
        print("⚠️ Atenção: A tabela veio vazia.")
        return

    # 4. Interpolação PCHIP
    print("➗ Calculando Interpolação (PCHIP)...")
    df = df.sort_values('Vertices').drop_duplicates(subset=['Vertices'])
    
    max_dias = df['Vertices'].max()
    novos_vertices = np.arange(1, max_dias + 1)
    
    pchip_ipca = PchipInterpolator(df['Vertices'], df['ETTJ_IPCA'])
    pchip_pre = PchipInterpolator(df['Vertices'], df['ETTJ_PREF'])
    pchip_inf = PchipInterpolator(df['Vertices'], df['Inflacao_Implicita'])
    
    # Gera o DataFrame final
    df_final = pd.DataFrame({
        'dias_corridos': novos_vertices,
        'taxa_ipca': pchip_ipca(novos_vertices),
        'taxa_pre': pchip_pre(novos_vertices),
        'inflacao_implicita': pchip_inf(novos_vertices)
    })
    
    # --- NOVO: ADICIONAR A COLUNA "E" (DATA) ---
    df_final['data_referencia'] = data_arquivo
    
    # 5. Salvar no Banco
    conn = sqlite3.connect('meu_app.db')
    
    # Salva tabela (substituindo a antiga)
    df_final.to_sql('curvas_anbima', conn, if_exists='replace', index=False)
    
    # Atualiza metadata
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS metadata (chave TEXT PRIMARY KEY, valor TEXT)")
    # Agora salvamos a data do ARQUIVO, não a data de hoje (hora do download)
    cursor.execute("INSERT OR REPLACE INTO metadata (chave, valor) VALUES ('ultima_atualizacao', ?)", (data_arquivo,))
    
    conn.commit()
    conn.close()
    
    print(f"💾 Sucesso! {len(df_final)} linhas salvas.")
    print(f"✅ Coluna de Data adicionada: {data_arquivo}")

if __name__ == "__main__":
    processar_dados_anbima()