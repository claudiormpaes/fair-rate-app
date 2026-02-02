import streamlit as st
import pandas as pd
import sqlite3
import numpy as np
import os
import altair as alt
from datetime import datetime

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="FAIR RATE", 
    page_icon="⚖️", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# ==============================================================================
# CSS MASTER V24 - BOTÕES INTELIGENTES & UI PREMIUM
# ==============================================================================
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;600;700&family=Open+Sans:wght@400;600&display=swap');
    
    /* --- FUNDAMENTOS --- */
    .stApp { 
        background-color: #0B1F3A !important; 
        margin-bottom: 80px;
    }
    
    /* --- MENUS E CONTROLES --- */
    /* Botão de Abrir Sidebar (Fixo Esquerda) */
    [data-testid="stSidebarCollapsedControl"] {
        display: flex !important;
        visibility: visible !important;
        position: fixed !important;
        top: 20px !important;
        left: 20px !important;
        z-index: 1000005 !important;
        background-color: #112240 !important;
        border: 1px solid #B89B5E !important;
        color: #B89B5E !important;
        border-radius: 8px !important;
        width: 40px !important;
        height: 40px !important;
        align-items: center;
        justify-content: center;
        transition: all 0.3s ease;
    }
    [data-testid="stSidebarCollapsedControl"]:hover {
        background-color: #B89B5E !important;
        color: #0B1F3A !important;
        transform: scale(1.1);
        cursor: pointer;
    }
    [data-testid="stSidebarCollapsedControl"] svg {
        fill: currentColor !important;
        stroke: currentColor !important;
    }
    
    /* Menu Superior Direito (3 Pontos) */
    [data-testid="stToolbar"] {
        visibility: visible !important;
        right: 20px; top: 10px;
    }
    [data-testid="stToolbar"] button { color: #B89B5E !important; }
    
    /* Esconde Deploy/Decoração */
    .stAppDeployButton, [data-testid="stDecoration"] { display: none; }
    
    /* --- TIPOGRAFIA --- */
    h1 { 
        font-family: 'Montserrat', sans-serif; 
        font-weight: 700; 
        font-size: 2.2rem !important; 
        color: #F5F1E8 !important; 
        letter-spacing: -0.5px;
    }
    h2, h3 { font-family: 'Montserrat', sans-serif; color: #B89B5E !important; }
    p, label, span, div { font-family: 'Open Sans', sans-serif; color: #E2E8F0; }
    
    /* --- SIDEBAR --- */
    section[data-testid="stSidebar"] {
        width: 380px !important;
        background-color: #050E1A !important;
        border-right: 1px solid rgba(184, 155, 94, 0.3);
    }
    
    /* --- CARDS --- */
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, #112240 0%, #0F1D36 100%);
        border: 1px solid #2C3E50;
        border-radius: 10px;
        padding: 18px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
    }
    div[data-testid="stMetricLabel"] { 
        color: #94A3B8 !important; 
        font-size: 14px !important; 
        font-weight: 600 !important;
        text-transform: uppercase; 
    }
    div[data-testid="stMetricValue"] { 
        color: #F5F1E8 !important; 
        font-family: 'Montserrat', sans-serif;
        font-weight: 700;
        font-size: 30px !important; 
    }
    div[data-testid="stMetricDelta"] {
        font-size: 14px !important;
        font-weight: 600;
    }
    
    /* --- BOTÃO DE AÇÃO --- */
    .stButton>button {
        background: linear-gradient(90deg, #B89B5E 0%, #D4B475 100%);
        color: #050E1A !important;
        font-family: 'Montserrat', sans-serif;
        font-weight: 800;
        text-transform: uppercase;
        border-radius: 6px;
        border: none;
        height: 55px;
        font-size: 16px;
        box-shadow: 0 4px 15px rgba(184, 155, 94, 0.3);
        transition: all 0.2s ease;
        width: 100%;
    }
    .stButton>button:hover { 
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(184, 155, 94, 0.5);
        color: #000 !important;
    }
    
    /* --- INPUTS --- */
    div[data-baseweb="select"] > div, input {
        background-color: #112240 !important;
        border: 1px solid #334155 !important;
        color: #F5F1E8 !important;
        border-radius: 6px;
        font-size: 15px;
    }
    div[data-baseweb="popover"], ul[role="listbox"] {
        background-color: #0B1829 !important;
        border: 1px solid #B89B5E !important;
    }
    li[role="option"] { color: #E6E2D8 !important; }
    li[role="option"]:hover, li[role="option"][aria-selected="true"] {
        background-color: #B89B5E !important;
        color: #050E1A !important;
    }

    /* --- BOTÕES PEQUENOS --- */
    div[data-testid="column"] button { 
        background-color: #1E293B !important;
        color: #B89B5E !important;
        border: 1px solid #334155 !important;
        font-weight: 600;
        font-size: 13px;
        padding: 5px;
    }
    div[data-testid="column"] button:hover {
        border-color: #B89B5E !important;
        color: #F5F1E8 !important;
    }

    /* --- RODAPÉ --- */
    .footer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #050E1A; 
        border-top: 1px solid #B89B5E;
        color: #94A3B8;
        text-align: center; padding: 15px; 
        font-size: 12px; z-index: 9999;
        box-shadow: 0 -4px 10px rgba(0,0,0,0.3);
    }
    .footer b { color: #B89B5E; }
    </style>
""", unsafe_allow_html=True)

# --- 2. FUNÇÕES DE DADOS ---
def get_datas_disponiveis():
    try:
        conn = sqlite3.connect('meu_app.db')
        query = "SELECT DISTINCT data_referencia FROM curvas_anbima"
        df_datas = pd.read_sql(query, conn)
        conn.close()
        if df_datas.empty: return []
        df_datas['data_dt'] = pd.to_datetime(df_datas['data_referencia'], format="%d/%m/%Y", errors='coerce')
        return df_datas.sort_values('data_dt', ascending=False)['data_referencia'].tolist()
    except:
        return []

def carregar_dados_por_data(data_selecionada):
    try:
        conn = sqlite3.connect('meu_app.db')
        query = "SELECT * FROM curvas_anbima WHERE data_referencia = ?"
        df = pd.read_sql(query, conn, params=(data_selecionada,))
        conn.close()
        return df
    except:
        return pd.DataFrame()

# --- 3. BARRA LATERAL ---
with st.sidebar:
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    logo_path = "FAIR RATE LOGO/1.png"
    if os.path.exists(logo_path):
        st.image(logo_path, use_container_width=True)
    else:
        st.markdown("<h1>FAIR RATE</h1>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    datas = get_datas_disponiveis()
    if not datas:
        st.error("⚠️ Base de dados vazia.")
        st.stop()
        
    st.caption("CONFIGURAÇÃO")
    data_escolhida = st.selectbox("Data Base", datas, index=0)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    st.caption("PARAMETROS DA PROPOSTA")
    tipo_oferta = st.selectbox("Indexador", ["Prefixado", "IPCA + Spread", "% do CDI", "CDI + Spread"])
    
    # --- NOVO: SELETOR DE UNIDADE ---
    unidade_prazo = st.selectbox("Unidade de Prazo", ["Meses", "Anos", "Dias Úteis"])
    
    # Lógica de Botões Dinâmicos
    if unidade_prazo == "Anos":
        b_labels = [1, 2, 5, 10]
        st.write("Prazo (Anos)")
    elif unidade_prazo == "Dias Úteis":
        b_labels = [252, 504, 1260, 2520] # Padrão Anbima (1, 2, 5, 10 anos)
        st.write("Prazo (Dias Úteis)")
    else: # Meses
        b_labels = [12, 24, 60, 120]
        st.write("Prazo (Meses)")
    
    c1, c2, c3, c4 = st.columns([1,1,1,1], gap="small")
    
    # Inicializa sessão se não existir
    if "prazo_selecionado" not in st.session_state: st.session_state.prazo_selecionado = b_labels[1] # Padrão: 24 meses / 2 anos
    
    def set_prazo(p): st.session_state.prazo_selecionado = p
    
    c1.button(str(b_labels[0]), on_click=set_prazo, args=(b_labels[0],))
    c2.button(str(b_labels[1]), on_click=set_prazo, args=(b_labels[1],))
    c3.button(str(b_labels[2]), on_click=set_prazo, args=(b_labels[2],))
    c4.button(str(b_labels[3]), on_click=set_prazo, args=(b_labels[3],))
    
    prazo_input = st.number_input("Input Manual", min_value=1, max_value=5000, value=st.session_state.prazo_selecionado, label_visibility="collapsed")
    
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    taxa_oferta = st.number_input("Taxa Oferecida (% a.a.)", value=13.00, step=0.1, format="%.2f")
    
    st.markdown("---")
    
    # 3.3 BOX DE CENÁRIO (CÁLCULO DINÂMICO DOS DIAS)
    df_curva = carregar_dados_por_data(data_escolhida)
    cdi_projetado = 0.0
    implicita_projetada = 0.0
    dias_target = 0
    
    if not df_curva.empty:
        # Conversão Universal para Dias Úteis (Motor do App)
        if unidade_prazo == "Meses":
            dias_target = int(prazo_input * 21)
        elif unidade_prazo == "Anos":
            dias_target = int(prazo_input * 252)
        else: # Dias Úteis
            dias_target = int(prazo_input)
            
        # Busca na Curva
        # Garante que não estoure o limite da curva
        max_dias_banco = df_curva['dias_corridos'].max()
        if dias_target > max_dias_banco:
            st.warning(f"⚠️ Prazo excede limite da curva ({max_dias_banco} dias). Usando máximo.")
            dias_target = max_dias_banco

        idx = (np.abs(df_curva['dias_corridos'] - dias_target)).argmin()
        cdi_projetado = df_curva.iloc[idx]['taxa_pre']
        implicita_projetada = df_curva.iloc[idx]['inflacao_implicita']
        
        st.caption("PROJEÇÃO ANBIMA")
        st.markdown(f"""
        <div style="background-color: #0F172A; padding: 16px; border-radius: 8px; border-left: 4px solid #B89B5E;">
            <div style="color: #94A3B8; font-size: 13px; font-weight: 600; margin-bottom: 5px;">CDI MÉDIO (FUTURO)</div>
            <div style="color: #F5F1E8; font-size: 22px; font-weight: bold;">{cdi_projetado:.2f}% <span style="font-size:14px; font-weight:normal">a.a.</span></div>
            <div style="margin-top: 12px; border-top: 1px solid #334155; padding-top: 10px;">
                <div style="color: #CBD5E1; font-size: 13px;">IPCA IMPLÍCITO: <span style="color: #B89B5E; font-weight: bold;">{implicita_projetada:.2f}%</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

# --- 4. ÁREA PRINCIPAL ---
if not df_curva.empty:
    taxa_nominal_final = 0.0
    taxa_real_usuario = 0.0 
    
    if tipo_oferta == "IPCA + Spread":
        taxa_nominal_final = ((1 + taxa_oferta/100) * (1 + implicita_projetada/100) - 1) * 100
        taxa_real_usuario = taxa_oferta
    elif tipo_oferta == "Prefixado":
        taxa_nominal_final = taxa_oferta
        taxa_real_usuario = ((1 + taxa_oferta/100) / (1 + implicita_projetada/100) - 1) * 100
    elif tipo_oferta == "% do CDI":
        taxa_nominal_final = (taxa_oferta/100) * cdi_projetado
        taxa_real_usuario = ((1 + taxa_nominal_final/100) / (1 + implicita_projetada/100) - 1) * 100
    elif tipo_oferta == "CDI + Spread":
        taxa_nominal_final = ((1 + cdi_projetado/100) * (1 + taxa_oferta/100) - 1) * 100
        taxa_real_usuario = ((1 + taxa_nominal_final/100) / (1 + implicita_projetada/100) - 1) * 100

    ipca_real_anbima = df_curva.iloc[idx]['taxa_ipca']
    eq_pct_cdi = (taxa_nominal_final / cdi_projetado) * 100
    eq_cdi_spread = ((1 + taxa_nominal_final/100) / (1 + cdi_projetado/100) - 1) * 100
    eq_ipca_spread = taxa_real_usuario

    # Configuração de Labels
    if tipo_oferta == "Prefixado":
        val_display_3 = eq_ipca_spread
        label_display_3 = "Equivalente Real (IPCA+)"
        val_user_chart = taxa_nominal_final
        val_bench_chart = cdi_projetado
        nome_user_chart = "Sua Taxa (Pré)"
        nome_bench_chart = "CDI Projetado (Ref)"
        spread = taxa_nominal_final - cdi_projetado
        texto_delta = "Acima do CDI" if spread >= 0 else "Abaixo do CDI"
    else:
        val_display_3 = taxa_nominal_final
        label_display_3 = "Equivalente Nominal (Pré)"
        val_user_chart = taxa_real_usuario
        val_bench_chart = ipca_real_anbima
        nome_user_chart = "Sua Taxa Real"
        nome_bench_chart = "Título Público (IPCA+)"
        spread = taxa_real_usuario - ipca_real_anbima
        texto_delta = "Acima do TPF" if spread >= 0 else "Abaixo do TPF"

    # --- LAYOUT ---
    st.title("CALCULADORA DE FAIR RATE")
    st.markdown(f"<p style='margin-top: -15px; color: #94A3B8;'>Análise de prêmio de risco com base na curva {data_escolhida}</p>", unsafe_allow_html=True)

    if st.button("CALCULAR FAIR RATE", type="primary", use_container_width=True):
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        st.markdown("### 1. CONVERSÃO DE TAXAS")
        c1, c2, c3 = st.columns(3)
        c1.metric(label="EQUIVALENTE % DO CDI", value=f"{eq_pct_cdi:.2f}%")
        c2.metric(label="EQUIVALENTE CDI +", value=f"CDI + {eq_cdi_spread:.2f}%")
        c3.metric(label=label_display_3.upper(), value=f"{val_display_3:.2f}%")
        
        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown("### 2. ANÁLISE DE COMPETITIVIDADE (ALPHA)")
        k1, k2, k3 = st.columns(3)
        
        k1.metric(label=nome_user_chart.upper(), value=f"{val_user_chart:.2f}%")
        k2.metric(label=nome_bench_chart.upper(), value=f"{val_bench_chart:.2f}%")
        
        cor_delta = "normal" if spread >= 0 else "inverse"
        k3.metric(label="SPREAD (ALPHA)", value=f"{spread:+.2f} p.p.", delta=texto_delta, delta_color=cor_delta)
        
        st.markdown("---")
        
        st.markdown("### 📍 MAPA DO MERCADO")
        st.caption("Posição da sua oferta em relação às curvas de juros da ANBIMA.")
        
        chart_data = df_curva[df_curva['dias_corridos'] % 2 == 0].copy().dropna()
        chart_data['Anos'] = chart_data['dias_corridos'] / 252
        
        chart_data = chart_data.rename(columns={
            'taxa_pre': 'Curva Prefixada',
            'taxa_ipca': 'Curva IPCA+ (Real)',
            'inflacao_implicita': 'Inflação Implícita'
        })
        
        base_melt = chart_data.melt('Anos', value_vars=['Curva Prefixada', 'Curva IPCA+ (Real)', 'Inflação Implícita'], 
                                    var_name='Curva', value_name='Taxa')
        
        domain = ['Curva Prefixada', 'Curva IPCA+ (Real)', 'Inflação Implícita']
        range_ = ['#3B82F6', '#F59E0B', '#64748B'] 
        
        lines = alt.Chart(base_melt).mark_line(strokeWidth=2.5).encode(
            x=alt.X('Anos', axis=alt.Axis(grid=False, labelColor='#CBD5E1', titleColor='#B89B5E')),
            y=alt.Y('Taxa', axis=alt.Axis(grid=True, gridColor='#1E293B', labelColor='#CBD5E1', titleColor='#B89B5E')),
            color=alt.Color('Curva', scale=alt.Scale(domain=domain, range=range_), legend=alt.Legend(orient='top', title=None, labelColor='#E2E8F0')),
            tooltip=['Anos', 'Curva', alt.Tooltip('Taxa', format='.2f')]
        )
        
        pt_user = alt.Chart(pd.DataFrame({'Anos': [dias_target/252], 'Taxa': [val_user_chart], 'L': ['Sua Oferta']})).mark_circle(
            size=300, color='#B89B5E', opacity=1, stroke='white', strokeWidth=2
        ).encode(x='Anos', y='Taxa', tooltip=['L', alt.Tooltip('Taxa', format='.2f')])
        
        pt_bench = alt.Chart(pd.DataFrame({'Anos': [dias_target/252], 'Taxa': [val_bench_chart], 'L': ['Ref. Mercado']})).mark_circle(
            size=150, color='#CBD5E1', opacity=0.5
        ).encode(x='Anos', y='Taxa', tooltip=['L', alt.Tooltip('Taxa', format='.2f')])
        
        final_chart = (lines + pt_bench + pt_user).properties(height=500).configure_view(strokeWidth=0).interactive()
        st.altair_chart(final_chart, use_container_width=True)

else:
    st.info("Aguardando base de dados...")

# --- RODAPÉ PROTEGIDO ---
footer_html = """
<div class="footer">
    <p><b>FAIR RATE © 2026 | Desenvolvido por Cláudio Paes</b><br>
    Todos os direitos reservados. É proibida a cópia, reprodução ou engenharia reversa deste software sem autorização.<br>
    <i>Disclaimer: Esta ferramenta tem caráter estritamente educativo e de simulação. Não constitui recomendação de investimento.</i></p>
</div>
"""
st.markdown(footer_html, unsafe_allow_html=True)