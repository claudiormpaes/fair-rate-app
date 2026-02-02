import os

# Caminho do arquivo problemático
pasta_config = ".streamlit"
arquivo_config = os.path.join(pasta_config, "config.toml")

# 1. Apagar o arquivo antigo se ele existir (para limpar o erro)
if os.path.exists(arquivo_config):
    try:
        os.remove(arquivo_config)
        print("🗑️ Arquivo config.toml corrompido foi deletado.")
    except Exception as e:
        print(f"Erro ao deletar: {e}")

# 2. Criar a pasta se não existir
if not os.path.exists(pasta_config):
    os.makedirs(pasta_config)

# 3. Conteúdo SEM ACENTOS e forçando UTF-8
# Removi os comentários com acentos para garantir compatibilidade total
conteudo_config = """
[theme]
base = "dark"
primaryColor = "#B89B5E"
backgroundColor = "#0B1F3A"
secondaryBackgroundColor = "#050E1A"
textColor = "#F5F1E8"
font = "sans serif"
"""

# 4. Salvar forçando o encoding UTF-8 (O SEGREDO ESTÁ AQUI)
try:
    with open(arquivo_config, "w", encoding="utf-8") as f:
        f.write(conteudo_config)
    print("✅ Novo arquivo config.toml criado com sucesso (UTF-8)!")
    print("Agora você pode rodar o 'Rodar App.bat' sem erros.")
except Exception as e:
    print(f"❌ Erro ao criar arquivo: {e}")