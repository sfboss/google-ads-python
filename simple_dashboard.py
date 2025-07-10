#!/usr/bin/env python
"""
Simple SEO Keyword Dashboard
Streamlit app for enriched keyword analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from pathlib import Path
import glob

# Page config
st.set_page_config(
    page_title="SEO Keyword Dashboard",
    page_icon="🔍",
    layout="wide"
)

def load_latest_data():
    """Load the latest enriched keyword data."""
    results_dir = Path("results")
    
    if not results_dir.exists():
        return None
    
    csv_files = list(results_dir.glob("enriched_keywords_*.csv"))
    
    if not csv_files:
        return None
    
    # Get latest file
    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
    
    try:
        df = pd.read_csv(latest_file)
        return df, latest_file.name
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def create_sparkline(data_str):
    """Create a mini sparkline chart."""
    if not data_str or data_str == "":
        return None
    
    try:
        values = [int(x) for x in data_str.split(",") if x.strip()]
        if len(values) < 2:
            return None
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=values,
            mode='lines',
            line=dict(color='#1f77b4', width=2),
            hovertemplate='Volume: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            height=50,
            margin=dict(l=0, r=0, t=0, b=0),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        return fig
    except:
        return None

def main():
    st.title("🔍 SEO Keyword Dashboard")
    st.caption("Google Ads Enhanced Keyword Analysis")
    
    # Load data
    data_result = load_latest_data()
    
    if data_result is None:
        st.warning("⏳ No enriched keyword data found. Run the enrichment script first.")
        st.code("python simple_keyword_enricher.py")
        return
    
    df, filename = data_result
    
    # Sidebar - Data info
    st.sidebar.header("📊 Data Overview")
    st.sidebar.metric("Total Keywords", f"{len(df):,}")
    
    if 'api_match' in df.columns:
        matches = df['api_match'].sum()
        match_rate = matches / len(df) * 100
        st.sidebar.metric("API Matches", f"{matches:,}", f"{match_rate:.1f}%")
    
    if 'has_historical_data' in df.columns:
        with_history = df['has_historical_data'].sum()
        st.sidebar.metric("With Historical Data", f"{with_history:,}")
    
    st.sidebar.caption(f"Data: {filename}")
    
    # Filters
    st.sidebar.header("🔍 Filters")
    
    # Only show filters if we have the necessary columns
    if 'avg_monthly_searches' in df.columns:
        min_volume = st.sidebar.number_input(
            "Min Monthly Searches",
            min_value=0,
            max_value=int(df['avg_monthly_searches'].max()),
            value=0
        )
        
        max_volume = st.sidebar.number_input(
            "Max Monthly Searches",
            min_value=min_volume,
            max_value=int(df['avg_monthly_searches'].max()),
            value=int(df['avg_monthly_searches'].max())
        )
        
        # Apply volume filter
        df_filtered = df[
            (df['avg_monthly_searches'] >= min_volume) & 
            (df['avg_monthly_searches'] <= max_volume)
        ]
    else:
        df_filtered = df
    
    if 'competition' in df.columns:
        competition_options = df['competition'].unique()
        selected_competition = st.sidebar.multiselect(
            "Competition Level",
            options=competition_options,
            default=competition_options
        )
        
        if selected_competition:
            df_filtered = df_filtered[df_filtered['competition'].isin(selected_competition)]
    
    # Main content
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'avg_monthly_searches' in df_filtered.columns:
            avg_vol = df_filtered['avg_monthly_searches'].mean()
            st.metric("Avg Monthly Searches", f"{avg_vol:,.0f}")
    
    with col2:
        if 'volume_trend_coef' in df_filtered.columns:
            avg_trend = df_filtered['volume_trend_coef'].mean()
            trend_direction = "📈" if avg_trend > 0 else "📉" if avg_trend < 0 else "➡️"
            st.metric("Avg Trend Coefficient", f"{avg_trend:.4f}", delta=trend_direction)
    
    with col3:
        if 'seasonality_score' in df_filtered.columns:
            avg_seasonality = df_filtered['seasonality_score'].mean()
            st.metric("Avg Seasonality", f"{avg_seasonality:.3f}")
    
    # Charts
    if len(df_filtered) > 0:
        st.header("📊 Analysis")
        
        tab1, tab2, tab3 = st.tabs(["Volume Distribution", "Competition Analysis", "Keyword Details"])
        
        with tab1:
            if 'avg_monthly_searches' in df_filtered.columns:
                # Volume histogram
                fig_hist = px.histogram(
                    df_filtered,
                    x='avg_monthly_searches',
                    title="Search Volume Distribution",
                    nbins=50
                )
                fig_hist.update_layout(height=400)
                st.plotly_chart(fig_hist, use_container_width=True)
                
                # Volume vs Trend scatter
                if 'volume_trend_coef' in df_filtered.columns:
                    fig_scatter = px.scatter(
                        df_filtered,
                        x='avg_monthly_searches',
                        y='volume_trend_coef',
                        title="Search Volume vs Trend Coefficient",
                        hover_data=['keyword']
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)
        
        with tab2:
            if 'competition' in df_filtered.columns:
                # Competition distribution
                comp_counts = df_filtered['competition'].value_counts()
                fig_pie = px.pie(
                    values=comp_counts.values,
                    names=comp_counts.index,
                    title="Competition Level Distribution"
                )
                st.plotly_chart(fig_pie, use_container_width=True)
                
                # Competition vs Volume
                if 'avg_monthly_searches' in df_filtered.columns:
                    fig_box = px.box(
                        df_filtered,
                        x='competition',
                        y='avg_monthly_searches',
                        title="Search Volume by Competition Level"
                    )
                    st.plotly_chart(fig_box, use_container_width=True)
        
        with tab3:
            st.subheader("🔍 Keyword Details")
            
            # Search functionality
            search_term = st.text_input("Search keywords:", "")
            
            if search_term:
                mask = df_filtered['keyword'].str.contains(search_term, case=False, na=False)
                display_df = df_filtered[mask]
            else:
                display_df = df_filtered
            
            # Sort options
            if 'avg_monthly_searches' in display_df.columns:
                sort_by = st.selectbox(
                    "Sort by:",
                    ['avg_monthly_searches', 'volume_trend_coef', 'seasonality_score', 'keyword'],
                    index=0
                )
                ascending = st.checkbox("Ascending", value=False)
                display_df = display_df.sort_values(sort_by, ascending=ascending)
            
            # Display table with sparklines
            st.write(f"Showing {len(display_df):,} keywords")
            
            # Prepare display columns
            display_cols = ['keyword']
            if 'avg_monthly_searches' in display_df.columns:
                display_cols.append('avg_monthly_searches')
            if 'competition' in display_df.columns:
                display_cols.append('competition')
            if 'volume_trend_coef' in display_df.columns:
                display_cols.append('volume_trend_coef')
            if 'seasonality_score' in display_df.columns:
                display_cols.append('seasonality_score')
            
            # Show top results
            top_results = display_df.head(100)
            
            for idx, row in top_results.iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**{row['keyword']}**")
                        
                        metrics_text = []
                        if 'avg_monthly_searches' in row:
                            metrics_text.append(f"Volume: {row['avg_monthly_searches']:,}")
                        if 'competition' in row:
                            metrics_text.append(f"Competition: {row['competition']}")
                        if 'volume_trend_coef' in row:
                            trend = row['volume_trend_coef']
                            trend_emoji = "📈" if trend > 0 else "📉" if trend < 0 else "➡️"
                            metrics_text.append(f"Trend: {trend:.4f} {trend_emoji}")
                        
                        if metrics_text:
                            st.caption(" | ".join(metrics_text))
                    
                    with col2:
                        # Show sparkline if available
                        if 'sparkline_data' in row and row['sparkline_data']:
                            sparkline_fig = create_sparkline(row['sparkline_data'])
                            if sparkline_fig:
                                st.plotly_chart(sparkline_fig, use_container_width=True)
                    
                    st.divider()
    
    else:
        st.warning("No keywords match the current filters.")

if __name__ == "__main__":
    main()
