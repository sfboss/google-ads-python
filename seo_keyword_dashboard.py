#!/usr/bin/env python
"""
SEO Keyword Dashboard with Google Ads Integration
Advanced Streamlit app with sparklines, trend analysis, and keyword enrichment
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
import base64
from datetime import datetime, timedelta
from typing import List, Dict, Any
import subprocess
import time
from pathlib import Path

# Import our enrichment engine
from keyword_enrichment_engine import KeywordEnrichmentEngine

# Configure Streamlit
st.set_page_config(
    page_title="SEO Keyword Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

class AdvancedKeywordDashboard:
    """Advanced SEO keyword dashboard with Google Ads integration."""
    
    def __init__(self):
        self.enricher = KeywordEnrichmentEngine()
        self.initialize_session_state()
    
    def initialize_session_state(self):
        """Initialize Streamlit session state variables."""
        defaults = {
            'enriched_df': None,
            'selected_keywords': [],
            'filter_config': {},
            'auto_enrich': False,
            'batch_size': 500,
            'enrichment_progress': 0,
            'enrichment_status': 'idle',
            'sparkline_config': {
                'height': 30,
                'color_positive': '#00ff00',
                'color_negative': '#ff0000',
                'color_neutral': '#888888'
            }
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    def create_sparkline_chart(self, data_string: str, trend_coef: float = 0) -> str:
        """Create a sparkline chart from comma-separated data."""
        if not data_string:
            return ""
        
        try:
            values = [float(x) for x in data_string.split(',') if x.strip()]
            if len(values) < 2:
                return ""
            
            # Determine color based on trend
            if trend_coef > 0.01:
                color = st.session_state.sparkline_config['color_positive']
            elif trend_coef < -0.01:
                color = st.session_state.sparkline_config['color_negative']
            else:
                color = st.session_state.sparkline_config['color_neutral']
            
            # Create mini line chart
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                y=values,
                mode='lines',
                line=dict(color=color, width=2),
                showlegend=False,
                hovertemplate='Volume: %{y}<extra></extra>'
            ))
            
            fig.update_layout(
                width=100,
                height=st.session_state.sparkline_config['height'],
                margin=dict(l=0, r=0, t=0, b=0),
                xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            
            # Convert to HTML
            config = {'displayModeBar': False}
            html = fig.to_html(config=config, div_id=f"sparkline_{hash(data_string)}")
            return html
            
        except Exception as e:
            st.error(f"Error creating sparkline: {e}")
            return ""
    
    def calculate_keyword_score(self, row: pd.Series) -> float:
        """Calculate composite keyword score based on multiple factors."""
        try:
            # Normalize metrics to 0-1 scale
            volume_score = min(row.get('avg_monthly_searches', 0) / 10000, 1.0)
            
            # Competition score (lower is better)
            comp_index = row.get('competition_index', 50)
            competition_score = max(0, 1 - (comp_index / 100))
            
            # Trend score
            trend_coef = row.get('volume_trend_coef', 0)
            trend_score = max(0, min(trend_coef * 10 + 0.5, 1.0))
            
            # Volatility score (lower volatility is better for some use cases)
            volatility = row.get('volatility_index', 0.5)
            volatility_score = max(0, 1 - volatility)
            
            # Weighted composite score
            score = (
                volume_score * 0.3 +
                competition_score * 0.25 +
                trend_score * 0.25 +
                volatility_score * 0.2
            )
            
            return round(score * 100, 1)  # Convert to 0-100 scale
            
        except Exception:
            return 0.0
    
    def create_trend_indicator(self, trend_coef: float) -> str:
        """Create trend indicator emoji."""
        if trend_coef > 0.02:
            return "📈"
        elif trend_coef < -0.02:
            return "📉"
        else:
            return "➡️"
    
    def format_currency(self, micros: int) -> str:
        """Format micro-currency to USD."""
        return f"${micros / 1_000_000:.2f}"
    
    def display_keyword_metrics_grid(self, df: pd.DataFrame):
        """Display key metrics in a grid layout."""
        if df is None or df.empty:
            st.warning("No data available for metrics.")
            return
        
        # Calculate metrics
        total_keywords = len(df)
        avg_volume = df['avg_monthly_searches'].mean()
        high_volume_count = (df['avg_monthly_searches'] > 1000).sum()
        positive_trends = (df['volume_trend_coef'] > 0.01).sum()
        high_competition = (df['competition_index'] > 70).sum()
        
        # Display metrics in columns
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                label="Total Keywords",
                value=f"{total_keywords:,}",
                delta=None
            )
        
        with col2:
            st.metric(
                label="Avg Monthly Searches",
                value=f"{avg_volume:.0f}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="High Volume (>1K)",
                value=f"{high_volume_count:,}",
                delta=f"{high_volume_count/total_keywords*100:.1f}%"
            )
        
        with col4:
            st.metric(
                label="Positive Trends",
                value=f"{positive_trends:,}",
                delta=f"{positive_trends/total_keywords*100:.1f}%"
            )
        
        with col5:
            st.metric(
                label="High Competition",
                value=f"{high_competition:,}",
                delta=f"{high_competition/total_keywords*100:.1f}%"
            )
    
    def create_advanced_keyword_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create enhanced keyword table with sparklines and calculated fields."""
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Add calculated columns
        df['keyword_score'] = df.apply(self.calculate_keyword_score, axis=1)
        df['trend_indicator'] = df['volume_trend_coef'].apply(self.create_trend_indicator)
        df['low_bid_usd'] = df['low_top_of_page_bid_micros'].apply(self.format_currency)
        df['high_bid_usd'] = df['high_top_of_page_bid_micros'].apply(self.format_currency)
        
        # Create display DataFrame
        display_columns = [
            'keyword',
            'keyword_score',
            'avg_monthly_searches',
            'trend_indicator',
            'volume_trend_coef',
            'seasonality_score',
            'volatility_index',
            'competition',
            'competition_index',
            'low_bid_usd',
            'high_bid_usd',
            'sparkline_data'
        ]
        
        available_columns = [col for col in display_columns if col in df.columns]
        display_df = df[available_columns].copy()
        
        # Format numeric columns
        numeric_format = {
            'keyword_score': '{:.1f}',
            'avg_monthly_searches': '{:,.0f}',
            'volume_trend_coef': '{:.4f}',
            'seasonality_score': '{:.3f}',
            'volatility_index': '{:.3f}',
            'competition_index': '{:.0f}'
        }
        
        for col, fmt in numeric_format.items():
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: fmt.format(x) if pd.notnull(x) else '')
        
        return display_df
    
    def display_enrichment_controls(self):
        """Display controls for keyword enrichment."""
        st.subheader("Keyword Enrichment Controls")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            batch_size = st.selectbox(
                "Batch Size",
                [100, 200, 300, 500, 800],
                index=3,  # Default to 500
                help="Number of keywords to process per API call"
            )
            st.session_state.batch_size = batch_size
        
        with col2:
            auto_enrich = st.checkbox(
                "Auto-enrich new keywords",
                value=st.session_state.auto_enrich,
                help="Automatically enrich keywords when added"
            )
            st.session_state.auto_enrich = auto_enrich
        
        with col3:
            st.write("Enrichment Status:")
            status_color = {
                'idle': '🟢',
                'running': '🟡',
                'complete': '🟢',
                'error': '🔴'
            }
            st.write(f"{status_color.get(st.session_state.enrichment_status, '⚪')} {st.session_state.enrichment_status.title()}")
    
    def enrich_keywords_interface(self, keywords: List[str]):
        """Interface for enriching keywords with progress tracking."""
        if not keywords:
            st.warning("No keywords to enrich.")
            return None
        
        st.write(f"Ready to enrich {len(keywords)} keywords in batches of {st.session_state.batch_size}")
        
        if st.button("Start Enrichment", type="primary"):
            st.session_state.enrichment_status = 'running'
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def progress_callback(batch_num, total_batches, response_time, status):
                progress = batch_num / total_batches
                progress_bar.progress(progress)
                status_text.text(f"Processing batch {batch_num}/{total_batches} ({response_time:.2f}s, {status})")
                st.session_state.enrichment_progress = progress
            
            try:
                # Run enrichment
                self.enricher.optimal_batch_size = st.session_state.batch_size
                enriched_df = self.enricher.enrich_keywords_bulk(keywords, progress_callback)
                
                # Save results
                output_file = self.enricher.save_enriched_data(enriched_df, "_dashboard")
                
                # Update session state
                st.session_state.enriched_df = enriched_df
                st.session_state.enrichment_status = 'complete'
                
                st.success(f"Enrichment complete! Results saved to {output_file}")
                
                return enriched_df
                
            except Exception as e:
                st.session_state.enrichment_status = 'error'
                st.error(f"Enrichment failed: {e}")
                return None
    
    def display_trend_analysis(self, df: pd.DataFrame):
        """Display trend analysis charts."""
        if df is None or df.empty:
            return
        
        st.subheader("Trend Analysis")
        
        # Create trend distribution chart
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                'Volume Trend Distribution',
                'Seasonality vs Volume',
                'Competition vs Bid Range',
                'Keyword Score Distribution'
            ]
        )
        
        # Trend distribution
        fig.add_trace(
            go.Histogram(x=df['volume_trend_coef'], name='Trend Coefficient'),
            row=1, col=1
        )
        
        # Seasonality vs Volume scatter
        fig.add_trace(
            go.Scatter(
                x=df['seasonality_score'],
                y=df['avg_monthly_searches'],
                mode='markers',
                name='Keywords',
                text=df['keyword'],
                hovertemplate='%{text}<br>Seasonality: %{x}<br>Volume: %{y}<extra></extra>'
            ),
            row=1, col=2
        )
        
        # Competition vs Bid Range
        fig.add_trace(
            go.Scatter(
                x=df['competition_index'],
                y=df['high_top_of_page_bid_micros'] / 1_000_000,
                mode='markers',
                name='Bid vs Competition',
                text=df['keyword'],
                hovertemplate='%{text}<br>Competition: %{x}<br>High Bid: $%{y}<extra></extra>'
            ),
            row=2, col=1
        )
        
        # Keyword Score Distribution
        if 'keyword_score' in df.columns:
            fig.add_trace(
                go.Histogram(x=df['keyword_score'], name='Keyword Score'),
                row=2, col=2
            )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    def display_filtering_controls(self, df: pd.DataFrame):
        """Display advanced filtering controls."""
        if df is None or df.empty:
            return df
        
        st.subheader("Advanced Filters")
        
        with st.expander("Filter Options", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                volume_range = st.slider(
                    "Monthly Searches",
                    min_value=int(df['avg_monthly_searches'].min()),
                    max_value=int(df['avg_monthly_searches'].max()),
                    value=(int(df['avg_monthly_searches'].min()), int(df['avg_monthly_searches'].max())),
                    step=10
                )
                
                competition_range = st.slider(
                    "Competition Index",
                    min_value=0,
                    max_value=100,
                    value=(0, 100),
                    step=5
                )
            
            with col2:
                trend_range = st.slider(
                    "Trend Coefficient",
                    min_value=float(df['volume_trend_coef'].min()),
                    max_value=float(df['volume_trend_coef'].max()),
                    value=(float(df['volume_trend_coef'].min()), float(df['volume_trend_coef'].max())),
                    step=0.001,
                    format="%.3f"
                )
                
                if 'keyword_score' in df.columns:
                    score_range = st.slider(
                        "Keyword Score",
                        min_value=0.0,
                        max_value=100.0,
                        value=(0.0, 100.0),
                        step=1.0
                    )
            
            with col3:
                competition_types = st.multiselect(
                    "Competition Level",
                    options=df['competition'].unique(),
                    default=df['competition'].unique()
                )
                
                has_historical = st.checkbox(
                    "Has Historical Data Only",
                    value=False
                )
        
        # Apply filters
        filtered_df = df.copy()
        
        # Apply numeric filters
        filtered_df = filtered_df[
            (filtered_df['avg_monthly_searches'] >= volume_range[0]) &
            (filtered_df['avg_monthly_searches'] <= volume_range[1]) &
            (filtered_df['competition_index'] >= competition_range[0]) &
            (filtered_df['competition_index'] <= competition_range[1]) &
            (filtered_df['volume_trend_coef'] >= trend_range[0]) &
            (filtered_df['volume_trend_coef'] <= trend_range[1])
        ]
        
        # Apply categorical filters
        filtered_df = filtered_df[filtered_df['competition'].isin(competition_types)]
        
        if has_historical and 'has_historical_data' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['has_historical_data'] == True]
        
        if 'keyword_score' in df.columns and 'score_range' in locals():
            filtered_df = filtered_df[
                (filtered_df['keyword_score'] >= score_range[0]) &
                (filtered_df['keyword_score'] <= score_range[1])
            ]
        
        st.write(f"Showing {len(filtered_df):,} of {len(df):,} keywords")
        
        return filtered_df
    
    def main_dashboard(self):
        """Main dashboard interface."""
        st.title("🚀 Advanced SEO Keyword Dashboard")
        st.markdown("*Powered by Google Ads API with Trend Analysis & Sparklines*")
        
        # Sidebar controls
        with st.sidebar:
            st.header("Dashboard Controls")
            
            # File upload
            uploaded_file = st.file_uploader(
                "Upload Keyword CSV",
                type=['csv'],
                help="Upload existing enriched keyword data"
            )
            
            if uploaded_file:
                try:
                    st.session_state.enriched_df = pd.read_csv(uploaded_file)
                    st.success(f"Loaded {len(st.session_state.enriched_df)} keywords")
                except Exception as e:
                    st.error(f"Error loading file: {e}")
            
            # Or load from master list
            if st.button("Load Master Keyword List"):
                keywords = self.enricher.load_keyword_master_list()
                if keywords:
                    st.write(f"Found {len(keywords)} keywords in master list")
                    
                    # Show enrichment interface
                    enriched_df = self.enrich_keywords_interface(keywords)
                    if enriched_df is not None:
                        st.session_state.enriched_df = enriched_df
        
        # Main content area
        if st.session_state.enriched_df is not None:
            df = st.session_state.enriched_df
            
            # Display enrichment controls
            self.display_enrichment_controls()
            
            # Display metrics
            self.display_keyword_metrics_grid(df)
            
            # Filtering controls
            filtered_df = self.display_filtering_controls(df)
            
            # Trend analysis
            self.display_trend_analysis(filtered_df)
            
            # Enhanced data table
            st.subheader("Keyword Data Table")
            display_df = self.create_advanced_keyword_table(filtered_df)
            
            # Configure data display
            st.data_editor(
                display_df,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "keyword_score": st.column_config.ProgressColumn(
                        "Score",
                        help="Composite keyword score (0-100)",
                        min_value=0,
                        max_value=100,
                    ),
                    "sparkline_data": st.column_config.LineChartColumn(
                        "Trend",
                        help="Monthly search volume trend (last 12 months)",
                    ),
                }
            )
            
            # Export options
            st.subheader("Export Options")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("Export Filtered Data"):
                    csv = filtered_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"filtered_keywords_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            with col2:
                if st.button("Export Summary Report"):
                    summary = self.enricher.generate_enrichment_summary(filtered_df)
                    summary_json = json.dumps(summary, indent=2)
                    st.download_button(
                        label="Download Report",
                        data=summary_json,
                        file_name=f"keyword_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
        
        else:
            # Welcome screen
            st.info("👆 Upload a keyword CSV file or load the master keyword list to get started!")
            
            st.markdown("""
            ### Features:
            - 🔍 **Google Ads Integration** - Real-time keyword data with optimal 500-keyword batching
            - 📈 **Trend Analysis** - Volume trend coefficients and seasonality scoring
            - ⚡ **Sparklines** - Visual trend indicators for each keyword
            - 🎯 **Smart Scoring** - Composite keyword scores based on multiple factors
            - 🔧 **Advanced Filtering** - Multi-dimensional filtering and search
            - 📊 **Interactive Charts** - Comprehensive data visualizations
            - 💾 **Export Options** - CSV and JSON export with summaries
            """)

def main():
    """Run the advanced keyword dashboard."""
    dashboard = AdvancedKeywordDashboard()
    dashboard.main_dashboard()

if __name__ == "__main__":
    main()
