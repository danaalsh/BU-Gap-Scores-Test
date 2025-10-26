#!/usr/bin/env python
# coding: utf-8

# In[1]:


import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np

st.set_page_config(page_title="BU Research Gap Analysis", layout="wide")

st.title("🔬 BU Research Gap Analysis")
st.markdown("### Identifying Strategic Investment Opportunities")

# Load data
@st.cache_data
def load_data():
    df = pd.read_csv("data/BU Gap Score.csv")
    df['Global_count'] = df['Global_count'].astype(str).str.replace(',', '').astype(float)
    df['BU_count (2024-2025)'] = pd.to_numeric(df['BU_count (2024-2025)'], errors='coerce')
    df['Gap Score'] = pd.to_numeric(df['Gap Score'], errors='coerce')
    
    # Shorten topic names
    topic_names = {
        "Artificial Intelligence in Healthcare / Artificial Intelligence in Healthcare and Education / Explainable Artificial Intelligence (XAI)": "AI in Healthcare",
        "AI in Service Interactions / AI in cancer detection": "AI in Cancer Detection",
        "Advanced Mathematical Modeling in Engineering / Advanced Nanomaterials in Catalysis / Spectral Theory in Mathematical Physics / advanced mathematical theories": "Advanced Math & Engineering",
        "Privacy-Preserving Technologies in Data": "Privacy Tech",
        "CAR-T cell therapy research": "CAR-T Therapy",
        "FinTech, Crowdfunding, Digital Finance": "FinTech",
        "Robotic Path Planning Algorithms": "Robotic Planning",
        "Extraction and Separation Processes": "Extraction Processes",
        "Electric Vehicles and Infrastructure": "Electric Vehicles",
        "Nanoplatforms for cancer theranostics": "Cancer Nanoplatforms",
        "Environmental Sustainability in Business": "Environmental Sustainability",
        "BIM and Construction Integration": "BIM & Construction",
        "Stock Market Forecasting Methods": "Stock Forecasting",
        "Black Holes and Theoretical Physics / Cold Atom Physics and Bose-Einstein Condensates / Physics of Superconductivity and Magnetism": "Theoretical Physics",
        "Spectroscopy and Chemometric Analyses": "Spectroscopy"
    }
    
    def get_short_name(full_name):
        return topic_names.get(full_name, full_name.split('/')[0].strip()[:30])
    
    df['Topic_Short'] = df['Primary Topic Id'].apply(get_short_name)
    df = df.sort_values('BU_count (2024-2025)', ascending=False).reset_index(drop=True)
    
    return df

df = load_data()

# Sidebar filters
st.sidebar.header("Filters")
min_global = st.sidebar.slider("Minimum Global Activity", 0, int(df['Global_count'].max()), 0)
max_bu = st.sidebar.slider("Maximum BU Activity (for opportunities)", 0, 100, 20)

# Filter data
df_filtered = df[(df['Global_count'] >= min_global) & (df['BU_count (2024-2025)'] <= max_bu)]

# Main visualization
st.subheader("📊 BU vs Global Research Activity")

# Create figure with Plotly (INTERACTIVE with hover!)
fig = go.Figure()

# X positions
x_positions = list(range(len(df_filtered)))

# Add Global counts (blue)
fig.add_trace(go.Scatter(
    x=x_positions,
    y=df_filtered['Global_count'],
    mode='markers',
    name='Global Count',
    marker=dict(size=12, color='#1f77b4', line=dict(width=2, color='black')),
    text=df_filtered['Topic_Short'],
    hovertemplate='<b>%{text}</b><br>' +
                  'Global Count: %{y:,}<br>' +
                  '<extra></extra>'
))

# Add BU counts (red)
fig.add_trace(go.Scatter(
    x=x_positions,
    y=df_filtered['BU_count (2024-2025)'],
    mode='markers',
    name='BU Count (2024-2025)',
    marker=dict(size=12, color='#d62728', line=dict(width=2, color='black'), symbol='diamond'),
    text=df_filtered['Topic_Short'],
    customdata=df_filtered['Gap Score'],
    hovertemplate='<b>%{text}</b><br>' +
                  'BU Count: %{y}<br>' +
                  'Gap Score: %{customdata:.6f}<br>' +
                  '<extra></extra>'
))

# Add connecting lines
for i in range(len(df_filtered)):
    fig.add_trace(go.Scatter(
        x=[i, i],
        y=[df_filtered.iloc[i]['BU_count (2024-2025)'], df_filtered.iloc[i]['Global_count']],
        mode='lines',
        line=dict(color='gray', width=1, dash='dot'),
        showlegend=False,
        hoverinfo='skip'
    ))

# Update layout
fig.update_layout(
    xaxis=dict(
        tickmode='array',
        tickvals=x_positions,
        ticktext=df_filtered['Topic_Short'],
        tickangle=45
    ),
    yaxis=dict(
        title='Count (log scale)',
        type='log'
    ),
    title='Hover over points for details!',
    height=600,
    hovermode='closest'
)

st.plotly_chart(fig, use_container_width=True)

# Investment opportunities
st.subheader("🎯 Top Investment Opportunities")
opportunities = df[df['BU_count (2024-2025)'] < 5].sort_values('Gap Score', ascending=False).head(5)

col1, col2, col3 = st.columns(3)

for idx, (i, row) in enumerate(opportunities.iterrows()):
    col = [col1, col2, col3][idx % 3]
    with col:
        st.metric(
            label=f"#{idx+1}: {row['Topic_Short']}", 
            value=f"BU: {int(row['BU_count (2024-2025)'])}",
            delta=f"Global: {int(row['Global_count']):,}"
        )
        st.caption(f"Gap Score: {row['Gap Score']:.6f}")

# Data table
st.subheader("📋 Full Data Table")
st.dataframe(
    df[['Topic_Short', 'BU_count (2024-2025)', 'Global_count', 'Gap Score']].rename(columns={
        'Topic_Short': 'Topic',
        'BU_count (2024-2025)': 'BU Count',
        'Global_count': 'Global Count'
    }),
    use_container_width=True,
    height=400a
)

# Download button
st.download_button(
    label="📥 Download Full Data (CSV)",
    data=df.to_csv(index=False),
    file_name="bu_gap_analysis.csv",
    mime="text/csv"
)


# In[ ]:




