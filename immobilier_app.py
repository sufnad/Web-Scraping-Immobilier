import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG & STYLING
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Immobilier France",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=Source+Sans+3:wght@300;400;500;600&display=swap');
    
    .stApp {
        background: linear-gradient(180deg, #0a0a0f 0%, #121218 50%, #0a0a0f 100%);
    }
    
    .main .block-container {
        padding: 2rem 3rem;
        max-width: 1400px;
    }
    
    h1, h2, h3 {
        font-family: 'Playfair Display', Georgia, serif !important;
        color: #f5f0e8 !important;
        letter-spacing: -0.02em;
    }
    
    h1 {
        font-size: 3.5rem !important;
        font-weight: 700 !important;
        background: linear-gradient(135deg, #f5f0e8 0%, #c9a227 50%, #f5f0e8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem !important;
        text-align: center;
    }
    
    h2 {
        font-size: 2rem !important;
        font-weight: 600 !important;
        border-bottom: 1px solid rgba(201, 162, 39, 0.3);
        padding-bottom: 0.5rem;
        margin-top: 2rem !important;
    }
    
    p, span, div, li {
        font-family: 'Source Sans 3', -apple-system, sans-serif !important;
        color: #b8b5ad !important;
    }
    
    .title-section {
        text-align: center;
        padding: 3rem 2rem 1rem 2rem;
    }
    
    .title-subtitle {
        font-family: 'Source Sans 3', sans-serif !important;
        font-size: 1.25rem;
        font-weight: 300;
        color: #8a8780 !important;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        margin-bottom: 1rem;
    }
    
    .presentation-section {
        background: rgba(20, 20, 28, 0.6);
        border: 1px solid rgba(201, 162, 39, 0.15);
        border-radius: 16px;
        padding: 2rem 2.5rem;
        margin: 2rem 0 3rem 0;
    }
    
    .presentation-section h3 {
        color: #c9a227 !important;
        margin-bottom: 1rem;
        font-size: 1.3rem !important;
    }
    
    .presentation-section p {
        font-size: 1.05rem;
        line-height: 1.8;
        color: #9a9790 !important;
    }
    
    .map-container {
        background: rgba(20, 20, 28, 0.8);
        border: 1px solid rgba(201, 162, 39, 0.2);
        border-radius: 20px;
        padding: 1rem;
        margin: 1rem 0;
        position: relative;
    }
    
    .map-overlay {
        position: absolute;
        top: 10px;
        right: 10px;
        background: rgba(201, 162, 39, 0.9);
        color: #0a0a0f;
        padding: 5px 12px;
        border-radius: 8px;
        font-size: 0.75rem;
        font-weight: 600;
        z-index: 1000;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
        background: transparent;
        border-bottom: 1px solid rgba(201, 162, 39, 0.2);
    }
    
    .stTabs [data-baseweb="tab"] {
        font-family: 'Source Sans 3', sans-serif !important;
        font-size: 1rem;
        font-weight: 500;
        color: #6a6860;
        background: transparent;
        border: none;
        padding: 1rem 0.5rem;
    }
    
    .stTabs [aria-selected="true"] {
        color: #c9a227 !important;
        border-bottom: 2px solid #c9a227;
    }
    
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(201, 162, 39, 0.3), transparent);
        margin: 3rem 0;
    }
    
    .info-box {
        background: rgba(201, 162, 39, 0.08);
        border-left: 3px solid #c9a227;
        padding: 1rem 1.5rem;
        border-radius: 0 12px 12px 0;
        margin: 1rem 0;
    }
    
    .stat-card {
        background: linear-gradient(145deg, rgba(30, 30, 40, 0.9), rgba(20, 20, 28, 0.9));
        border: 1px solid rgba(201, 162, 39, 0.2);
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
    }
    
    .stat-value {
        font-family: 'Playfair Display', serif !important;
        font-size: 2rem;
        font-weight: 700;
        color: #c9a227 !important;
    }
    
    .stat-label {
        font-size: 0.85rem;
        color: #6a6860 !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    iframe {
        border-radius: 16px !important;
        border: none !important;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def load_html_map(filepath):
    """Load an HTML file and return its content."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

@st.cache_data
def load_data(filepath):
    """Load and cache the CSV data."""
    df = pd.read_csv(filepath, sep=',', low_memory=False)
    # Convert numeric columns
    numeric_cols = ['surface_terrain', 'surface_interieure', 'surface_exterieure', 
                    'nombre_de_pieces', 'prix', 'prix_m2']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def create_correlation_chart(df):
    """Create scatter plot showing correlation between surface and price."""
    # Filter valid data
    df_valid = df.dropna(subset=['surface_interieure', 'prix']).copy()
    df_valid = df_valid[(df_valid['surface_interieure'] > 0) & (df_valid['prix'] > 0)]
    
    # Limit to reasonable values for visualization
    df_valid = df_valid[df_valid['surface_interieure'] <= 500]
    df_valid = df_valid[df_valid['prix'] <= 2000000]
    
    # Sample if too large
    if len(df_valid) > 5000:
        df_valid = df_valid.sample(n=5000, random_state=42)
    
    fig = px.scatter(
        df_valid,
        x='surface_interieure',
        y='prix',
        opacity=0.5,
        trendline='ols',
        labels={
            'surface_interieure': 'Surface intérieure (m²)',
            'prix': 'Prix (€)'
        },
        title='Corrélation entre surface et prix'
    )
    
    # Style the points
    fig.update_traces(marker=dict(color='#c9a227', size=6), selector=dict(mode='markers'))
    # Style the trendline
    fig.update_traces(line=dict(color='#e74c3c', width=2), selector=dict(mode='lines'))
    
    fig.update_layout(
        plot_bgcolor='rgba(20, 20, 28, 0.8)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#b8b5ad', family='Source Sans 3'),
        title=dict(font=dict(size=18, color='#f5f0e8', family='Playfair Display')),
        showlegend=False,
        xaxis=dict(gridcolor='rgba(201, 162, 39, 0.1)', zerolinecolor='rgba(201, 162, 39, 0.2)'),
        yaxis=dict(gridcolor='rgba(201, 162, 39, 0.1)', zerolinecolor='rgba(201, 162, 39, 0.2)')
    )
    
    return fig

def create_price_histogram_by_city(df):
    """Create histogram of price per m² for top cities."""
    df_valid = df.dropna(subset=['ville', 'prix_m2']).copy()
    df_valid = df_valid[(df_valid['prix_m2'] > 0) & (df_valid['prix_m2'] < 20000)]
    
    # Get top 10 cities by number of listings
    top_cities = df_valid['ville'].value_counts().head(10).index.tolist()
    df_top = df_valid[df_valid['ville'].isin(top_cities)]
    
    fig = px.box(
        df_top,
        x='ville',
        y='prix_m2',
        color='ville',
        labels={
            'ville': 'Ville',
            'prix_m2': 'Prix au m² (€)'
        },
        title='Distribution du prix au m² par ville (Top 10)'
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(20, 20, 28, 0.8)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#b8b5ad', family='Source Sans 3'),
        title=dict(font=dict(size=18, color='#f5f0e8', family='Playfair Display')),
        showlegend=False,
        xaxis=dict(gridcolor='rgba(201, 162, 39, 0.1)', tickangle=45),
        yaxis=dict(gridcolor='rgba(201, 162, 39, 0.1)')
    )
    
    return fig

def create_department_chart(df):
    """Create chart showing price per m² by department."""
    df_valid = df.dropna(subset=['departement', 'prix_m2']).copy()
    df_valid = df_valid[(df_valid['prix_m2'] > 0) & (df_valid['prix_m2'] < 20000)]
    
    # Aggregate by department
    dept_data = df_valid.groupby('departement').agg(
        prix_moyen_m2=('prix_m2', 'mean'),
        nombre_annonces=('departement', 'count')
    ).reset_index()
    
    # Sort by price and take top 15
    dept_data = dept_data.sort_values('prix_moyen_m2', ascending=True).tail(15)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=dept_data['departement'],
        x=dept_data['prix_moyen_m2'],
        orientation='h',
        marker=dict(
            color=dept_data['prix_moyen_m2'],
            colorscale=[[0, '#2ecc71'], [0.25, '#f1c40f'], [0.5, '#3498db'], [0.75, '#9b59b6'], [1, '#e74c3c']],
            colorbar=dict(
                title='Prix/m²',
                tickformat='€,.0f'
            )
        ),
        text=[f"{x:,.0f}€" for x in dept_data['prix_moyen_m2']],
        textposition='outside',
        textfont=dict(color='#b8b5ad'),
        hovertemplate='<b>%{y}</b><br>Prix moyen: %{x:,.0f}€/m²<br>Annonces: %{customdata}<extra></extra>',
        customdata=dept_data['nombre_annonces']
    ))
    
    fig.update_layout(
        title='Prix moyen au m² par département (Top 15)',
        plot_bgcolor='rgba(20, 20, 28, 0.8)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#b8b5ad', family='Source Sans 3'),
        title_font=dict(size=18, color='#f5f0e8', family='Playfair Display'),
        xaxis=dict(
            title='Prix moyen au m² (€)',
            gridcolor='rgba(201, 162, 39, 0.1)'
        ),
        yaxis=dict(
            title='',
            gridcolor='rgba(201, 162, 39, 0.1)'
        ),
        margin=dict(l=150)
    )
    
    return fig

# ─────────────────────────────────────────────────────────────────────────────
# MAIN APPLICATION
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ─────────────────────────────────────────────────────────────────────────
    # TITLE SECTION
    # ─────────────────────────────────────────────────────────────────────────
    
    st.markdown("""
    <div class="title-section">
        <p class="title-subtitle">Analyse du marché immobilier</p>
        <h1>Immobilier France</h1>
    </div>
    """, unsafe_allow_html=True)
    
    # ─────────────────────────────────────────────────────────────────────────
    # PRESENTATION SECTION
    # ─────────────────────────────────────────────────────────────────────────
    
    st.markdown("""
    <div class="presentation-section">
        <h3>📋 Présentation du projet</h3>
        <p>
            [Votre description ici - Cette section est réservée pour votre présentation personnalisée 
            du projet, méthodologie, sources de données, objectifs, etc.]
        </p>
        <p>
            [Vous pouvez ajouter plusieurs paragraphes pour expliquer le contexte, 
            les données utilisées, et les insights que vous souhaitez mettre en avant.]
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ─────────────────────────────────────────────────────────────────────────
    # DATA LOADING
    # ─────────────────────────────────────────────────────────────────────────
    
    data_path = "clean_data.csv"
    df = None
    
    if os.path.exists(data_path):
        df = load_data(data_path)
    
    # ─────────────────────────────────────────────────────────────────────────
    # STATISTICS SECTION
    # ─────────────────────────────────────────────────────────────────────────
    
    st.markdown("## 📊 Statistiques")
    
    if df is not None:
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{len(df):,}</div>
                <div class="stat-label">Annonces</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            avg_price = df['prix'].mean()
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{avg_price/1000:,.0f}k€</div>
                <div class="stat-label">Prix moyen</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            avg_price_m2 = df['prix_m2'].mean()
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{avg_price_m2:,.0f}€</div>
                <div class="stat-label">Prix moyen/m²</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            avg_surface = df['surface_interieure'].mean()
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{avg_surface:,.0f}m²</div>
                <div class="stat-label">Surface moyenne</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Charts
        chart_tab1, chart_tab2, chart_tab3 = st.tabs([
            "📈 Corrélation Surface/Prix", 
            "🏙️ Prix par Ville", 
            "🗺️ Prix par Département"
        ])
        
        with chart_tab1:
            fig_correlation = create_correlation_chart(df)
            st.plotly_chart(fig_correlation, use_container_width=True)
        
        with chart_tab2:
            fig_histogram = create_price_histogram_by_city(df)
            st.plotly_chart(fig_histogram, use_container_width=True)
        
        with chart_tab3:
            fig_dept = create_department_chart(df)
            st.plotly_chart(fig_dept, use_container_width=True)
    
    else:
        st.warning(f"⚠️ Fichier de données introuvable : `{data_path}`. Les statistiques ne peuvent pas être affichées.")
    
    st.markdown("---")
    
    # ─────────────────────────────────────────────────────────────────────────
    # MAPS SECTION
    # ─────────────────────────────────────────────────────────────────────────
    
    st.markdown("## 🗺️ Cartographie des prix")
    
    st.markdown("""
    <div class="info-box">
        <p style="margin: 0; color: #c9a227 !important;">
            <strong>Légende :</strong> La couleur représente le prix moyen au m², 
            la taille des cercles indique le nombre d'annonces.<br>
            <strong>💡 Astuce :</strong> Utilisez Ctrl + molette pour zoomer, ou cliquez en dehors de la carte pour la quitter.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Map file paths
    france_map_path = "carte_prix_moyen_dynamique.html"
    paris_map_path = "carte_prix_paris_par_arrondissement.html"
    
    map_tab1, map_tab2 = st.tabs(["🇫🇷 France par département", "🗼 Paris par arrondissement"])
    
    # ─── TAB 1: CARTE FRANCE ───
    with map_tab1:
        st.markdown("### Prix immobiliers par département")
        
        if os.path.exists(france_map_path):
            html_content = load_html_map(france_map_path)
            # Wrap map in a container that allows scrolling past it
            st.markdown('<div class="map-container">', unsafe_allow_html=True)
            components.html(html_content, height=600, scrolling=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.error(f"⚠️ Fichier carte introuvable : `{france_map_path}`")
    
    # ─── TAB 2: CARTE PARIS ───
    with map_tab2:
        st.markdown("### Prix immobiliers par arrondissement parisien")
        
        if os.path.exists(paris_map_path):
            html_content = load_html_map(paris_map_path)
            st.markdown('<div class="map-container">', unsafe_allow_html=True)
            components.html(html_content, height=600, scrolling=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.error(f"⚠️ Fichier carte introuvable : `{paris_map_path}`")
    
    # ─────────────────────────────────────────────────────────────────────────
    # FOOTER
    # ─────────────────────────────────────────────────────────────────────────
    
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0; color: #4a4a4a;">
        <p style="font-size: 0.85rem; margin: 0;">
            Données immobilières • Visualisation interactive
        </p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()