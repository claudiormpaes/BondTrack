"""
Utilitários compartilhados para a Sidebar do BondTrack
Garante consistência visual em todas as páginas
"""
import streamlit as st
import os

# Caminho da logo (relativo ao projeto - múltiplas tentativas)
def _get_logo_path():
    """Encontra o caminho da logo em diferentes localizações possíveis"""
    possible_paths = [
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "logo.png"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logo.png"),
        os.path.join(os.getcwd(), "logo.png"),
        "logo.png"
    ]
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None

LOGO_PATH = _get_logo_path()


def render_logo():
    """
    Renderiza a logo do BondTrack no topo da sidebar
    Deve ser chamado no início de cada página, dentro do bloco 'with st.sidebar:'
    """
    logo_path = _get_logo_path()  # Busca novamente em caso de mudança de diretório
    if logo_path and os.path.exists(logo_path):
        try:
            st.image(logo_path, width=250)
        except Exception:
            st.markdown("## 📊 BondTrack")
    else:
        st.markdown("## 📊 BondTrack")
    st.markdown("---")


def render_sidebar_footer():
    """
    Renderiza o rodapé da sidebar com informações do sistema
    """
    st.divider()
    st.markdown("### Sobre")
    st.info("""
    **BondTrack v1.2**
    
    Análise profissional de debêntures.
    
    Dados: SND + ANBIMA  
    Curva: ANBIMA  
    Volume: SND
    """)
