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
    initial_sidebar_state="collapsed" # Esconde a sidebar nativa se ela ainda existir
)

# ==============================================================================
# CSS MASTER V29 - LAYOUT HÍBRIDO & CARDS EMPILHADOS
# ==============================================================================
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;600;700&family=Open+Sans:wght@400;600&display=swap');
    
    /* --- FUNDAMENTOS --- */
    .stApp { 
        background-color: #0B1F3A !important; 
        margin-bottom: 80px;
    }
    
    /* Remove elementos nativos desnecessários */
    [data-testid="stSidebarCollapsedControl"] { display: none; }
    [data-testid="stToolbar"] { display: none; }
    .stAppDeployButton, [data-testid="stDecoration"] { display: none; }
    
    /* --- TIPOGRAFIA --- */
    h1 { 
        font-family: 'Montserrat', sans-serif; 
        font-weight: 700; 
        font-size: 2.2rem !important; 
        color: #F5F1E8 !important; 
        letter-spacing: -0.5px;
    }
    h2, h3, h4 { font-family: 'Montserrat', sans-serif; color: #B89B5E !important; }
    p, label, span, div { font-family: 'Open Sans', sans-serif; color: #E2E8F0; }
    
    /* --- CAIXA DE CONFIGURAÇÃO (A "NOVA" SIDEBAR) --- */
    .config-box {
        background-color: #050E1A;
        border: 1px solid rgba(184, 155, 94, 0.3);
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 20px;
    }

    /* --- CARDS DE RESULTADOS --- */
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, #112240 0%, #0F1D36 100%);
        border: 1px solid #2C3E50;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
        height: 100%;
        min-height: 110px;
    }
    div[data-testid="stMetricLabel"] { 
        color: #94A3B8 !important; 
        font-size: 13px !important; 
        font-weight: 600 !important;
        text-transform: uppercase; 
        white-space: normal !important;
        overflow-wrap: break-word;
        line-height: 1.2;
    }
    div[data-testid="stMetricValue"] { 
        color: #F5F1E8 !important; 
        font-family: 'Montserrat', sans-serif;
        font-weight: 700;
        font-size: 26px !important; 
    }
    
    /* --- BOTÕES --- */
    .stButton>button {
        background: linear-gradient(90deg, #B89B5E 0%, #D4B475 100%);
        color: #050E1A !important;
        font-family: 'Montserrat', sans-serif;
        font-weight: 800;
        text-transform: uppercase;
        border-radius: 6px;
        border: none;
        height: 50px;
        font-size: 15px;
        box-shadow: 0 4px 15px rgba(184, 155, 94, 0.3);
        transition: all 0.2s ease;
        width: 100%;
    }
    
    /* Inputs */
    div[data-baseweb="select"] > div, input {
        background-color: #112240 !important;
        border: 1px solid #334155 !important;
        color: #F5F1E8 !important;
        border-radius: 6px;
        font-size: 15px;
    }
    
    /* Botões pequenos de prazo */
    div[data-testid="column"] button { 
        background-color: #1E293B !important;
        color: #B89B5E !important;
        border: 1px solid #334155 !important;
        font-weight: 600;
        font-size: 12px;
        padding: 2px;
    }

    .footer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #050E1A; 
        border-top: 1px solid #B89B5E;
        color: #94A3B8;
        text-align: center; padding: 15px; 
        font-size: 11px; z-index: 9999;
    }

    /* ============================================================
       MOBILE OPTIMIZATION (BREAKPOINT: 800px)
       ============================================================ */
    @media (max-width: 800px) {
        h1 { font-size: 1.6rem !important; margin-bottom: 20px !important; }
        
        /* Força os Cards a ficarem um por linha no celular */
        [data-testid="metric-container"] {
            width: 100% !important;
        }
        
        /* Ajuste do container principal */
        .block-container {
            padding-top: 2rem !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        
        /* No celular, os inputs ficam no topo. Vamos dar um destaque neles */
        .config-box {
            border: 1px solid #B89B5E; /* Borda mais visível no celular */
            margin-bottom: 30px;
        }
        
        /* Garante que colunas de cards quebrem linha */
        div[data-testid="column"] {
            min-width: 100% !important;
            margin-bottom: 10px !important;
        }
        
        /* Exceção: Botões de prazo (12, 24...) devem ficar lado a lado */
        .prazo-buttons div[data-testid="column"] {
            min-width: 20% !important; /* Permite 4 botões na linha */
            margin-bottom: 0px !important;
        }
    }
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

# --- 3. LAYOUT PRINCIPAL (SEM SIDEBAR) ---

# Topo
c_logo, c_title = st.columns([0.5, 3])
with c_logo:
    logo_path = "FAIR RATE LOGO/1.png"
    if os.path.exists(logo_path):
        st.image(logo_path, use_container_width=True)
with c_title:
    st.title("FAIR RATE")
    st.markdown("<div style='margin-top: -15px; color: #94A3B8; font-size: 14px;'>Simulador Profissional de Curvas de Juros</div>", unsafe_allow_html=True)

st.divider()

# --- AQUI ESTÁ A MÁGICA: COLUNAS ASSIMÉTRICAS ---
# Desktop: Esquerda (Inputs) | Direita (Resultados)
# Mobile: Topo (Inputs) | Baixo (Resultados)
col_inputs, col_results = st.columns([1, 2.5], gap="large")

# --- COLUNA DA ESQUERDA (INPUTS) ---
with col_inputs:
    st.markdown('<div class="config-box">', unsafe_allow_html=True)
    st.markdown("### ⚙️ PARÂMETROS")
    
    datas = get_datas_disponiveis()
    if not datas:
        st.error("⚠️ Base vazia.")
        st.stop()
        
    data_escolhida = st.selectbox("Data Base", datas, index=0)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    tipo_oferta = st.selectbox("Indexador", ["Prefixado", "IPCA + Spread", "% do CDI", "CDI + Spread"])
    unidade_prazo = st.selectbox("Unidade de Prazo", ["Meses", "Anos", "Dias Úteis"])
    
    # Define labels
    if unidade_prazo == "Anos":
        b_labels = [1, 2, 5, 10]
        lbl_prazo = "Anos"
    elif unidade_prazo == "Dias Úteis":
        b_labels = [252, 504, 1260, 2520] 
        lbl_prazo = "Dias"
    else: 
        b_labels = [12, 24, 60, 120]
        lbl_prazo = "Meses"
    
    st.write(f"Prazo ({lbl_prazo})")
    
    # Container especial para os botões não quebrarem linha no CSS
    with st.container():
        st.markdown('<div class="prazo-buttons">', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        if "prazo_selecionado" not in st.session_state: st.session_state.prazo_selecionado = b_labels[1] 
        
        def set_prazo(p): st.session_state.prazo_selecionado = p
        
        c1.button(str(b_labels[0]), on_click=set_prazo, args=(b_labels[0],), use_container_width=True)
        c2.button(str(b_labels[1]), on_click=set_prazo, args=(b_labels[1],), use_container_width=True)
        c3.button(str(b_labels[2]), on_click=set_prazo, args=(b_labels[2],), use_container_width=True)
        c4.button(str(b_labels[3]), on_click=set_prazo, args=(b_labels[3],), use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    prazo_input = st.number_input("Input Manual", min_value=1, max_value=5000, value=st.session_state.prazo_selecionado, label_visibility="collapsed")
    
    st.markdown("<br>", unsafe_allow_html=True)
    taxa_oferta = st.number_input("Taxa Oferecida (% a.a.)", value=13.00, step=0.1, format="%.2f")
    
    st.markdown("<br>", unsafe_allow_html=True)
    calcular_btn = st.button("CALCULAR FAIR RATE", type="primary", use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True) # Fim config-box

# --- COLUNA DA DIREITA (RESULTADOS) ---
with col_results:
    # Processamento de dados
    df_curva = carregar_dados_por_data(data_escolhida)
    cdi_projetado = 0.0
    implicita_projetada = 0.0
    dias_target = 0
    idx = 0
    
    if not df_curva.empty:
        if unidade_prazo == "Meses":
            dias_target = int(prazo_input * 21)
        elif unidade_prazo == "Anos":
            dias_target = int(prazo_input * 252)
        else: 
            dias_target = int(prazo_input)
            
        max_dias_banco = df_curva['dias_corridos'].max()
        if dias_target > max_dias_banco:
            st.warning(f"⚠️ Prazo excede limite ({max_dias_banco} dias). Usando máximo.")
            dias_target = max_dias_banco

        idx = (np.abs(df_curva['dias_corridos'] - dias_target)).argmin()
        cdi_projetado = df_curva.iloc[idx]['taxa_pre']
        implicita_projetada = df_curva.iloc[idx]['inflacao_implicita']
        
        # BOX CENÁRIO PROJETADO
        st.markdown(f"""
        <div style="background-color: #0F172A; padding: 15px; border-radius: 8px; border-left: 4px solid #B89B5E; margin-bottom: 20px;">
            <span style="color: #94A3B8; font-size: 12px; font-weight: 600;">CENÁRIO ANBIMA ({data_escolhida})</span><br>
            <span style="color: #F5F1E8; font-size: 18px; font-weight: bold;">CDI PROJETADO: {cdi_projetado:.2f}%</span> 
            <span style="color: #64748B;">|</span> 
            <span style="color: #B89B5E;">IPCA IMPLÍCITO: {implicita_projetada:.2f}%</span>
        </div>
        """, unsafe_allow_html=True)

        # CÁLCULOS
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

        if tipo_oferta == "Prefixado":
            val_display_3 = eq_ipca_spread
            label_display_3 = "Equiv. Real (IPCA+)"
            val_user_chart = taxa_nominal_final
            val_bench_chart = cdi_projetado
            nome_user_chart = "Sua Taxa (Pré)"
            nome_bench_chart = "CDI Projetado"
            spread = taxa_nominal_final - cdi_projetado
            texto_delta = "Acima do CDI" if spread >= 0 else "Abaixo do CDI"
        else:
            val_display_3 = taxa_nominal_final
            label_display_3 = "Equiv. Nominal (Pré)"
            val_user_chart = taxa_real_usuario
            val_bench_chart = ipca_real_anbima
            nome_user_chart = "Sua Taxa Real"
            nome_bench_chart = "Tesouro IPCA+"
            spread = taxa_real_usuario - ipca_real_anbima
            texto_delta = "Acima do TPF" if spread >= 0 else "Abaixo do TPF"

        # --- CARDS DE RESULTADO ---
        st.markdown("#### 1. EQUIVALÊNCIAS")
        k1, k2, k3 = st.columns(3)
        k1.metric("EM % DO CDI", f"{eq_pct_cdi:.2f}%")
        k2.metric("EM CDI +", f"CDI + {eq_cdi_spread:.2f}%")
        k3.metric(label_display_3, f"{val_display_3:.2f}%")
        
        st.markdown("#### 2. ALPHA (SPREAD)")
        a1, a2, a3 = st.columns(3)
        a1.metric(nome_user_chart, f"{val_user_chart:.2f}%")
        a2.metric(nome_bench_chart, f"{val_bench_chart:.2f}%")
        
        cor_delta = "normal" if spread >= 0 else "inverse"
        a3.metric("GANHO REAL (ALPHA)", f"{spread:+.2f} p.p.", delta=texto_delta, delta_color=cor_delta)
        
        st.divider()
        st.markdown("#### 📍 MAPA DO MERCADO")
        
        # GRÁFICO
        chart_data = df_curva[df_curva['dias_corridos'] % 2 == 0].copy().dropna()
        chart_data['Anos'] = chart_data['dias_corridos'] / 252
        
        chart_data = chart_data.rename(columns={
            'taxa_pre': 'Curva Pré',
            'taxa_ipca': 'Curva IPCA+',
            'inflacao_implicita': 'Inflação Impl.'
        })
        
        base_melt = chart_data.melt('Anos', value_vars=['Curva Pré', 'Curva IPCA+', 'Inflação Impl.'], 
                                    var_name='Curva', value_name='Taxa')
        
        domain = ['Curva Pré', 'Curva IPCA+', 'Inflação Impl.']
        range_ = ['#3B82F6', '#F59E0B', '#64748B'] 
        
        lines = alt.Chart(base_melt).mark_line(strokeWidth=2.5).encode(
            x=alt.X('Anos', axis=alt.Axis(grid=False, labelColor='#CBD5E1', titleColor='#B89B5E')),
            y=alt.Y('Taxa', axis=alt.Axis(grid=True, gridColor='#1E293B', labelColor='#CBD5E1', titleColor='#B89B5E')),
            color=alt.Color('Curva', scale=alt.Scale(domain=domain, range=range_), 
                            legend=alt.Legend(orient='bottom', title=None, labelColor='#E2E8F0')),
            tooltip=['Anos', 'Curva', alt.Tooltip('Taxa', format='.2f')]
        )
        
        pt_user = alt.Chart(pd.DataFrame({'Anos': [dias_target/252], 'Taxa': [val_user_chart], 'L': ['Sua Oferta']})).mark_circle(
            size=200, color='#B89B5E', opacity=1, stroke='white', strokeWidth=2
        ).encode(x='Anos', y='Taxa', tooltip=['L', alt.Tooltip('Taxa', format='.2f')])
        
        pt_bench = alt.Chart(pd.DataFrame({'Anos': [dias_target/252], 'Taxa': [val_bench_chart], 'L': ['Benchmark']})).mark_circle(
            size=150, color='#CBD5E1', opacity=0.5
        ).encode(x='Anos', y='Taxa', tooltip=['L', alt.Tooltip('Taxa', format='.2f')])
        
        final_chart = (lines + pt_bench + pt_user).properties(height=400).configure_view(strokeWidth=0).interactive()
        st.altair_chart(final_chart, use_container_width=True)

# --- RODAPÉ ---
footer_html = """
<div class="footer">
    <p><b>FAIR RATE © 2026 | Desenvolvido por Cláudio Paes</b><br>
    <i>Disclaimer: Ferramenta educativa. Não constitui recomendação de investimento.</i></p>
</div>
"""
st.markdown(footer_html, unsafe_allow_html=True)
