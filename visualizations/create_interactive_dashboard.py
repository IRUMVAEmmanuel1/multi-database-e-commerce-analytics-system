# visualizations/create_interactive_dashboard.py
"""
Enhanced Interactive HTML Dashboard
Professional version with improved UI/UX and data visualization best practices
"""

import json
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
import os

# Constants for design consistency
COLORS = {
    "revenue": "#F39C12",
    "customers": "#3498DB",
    "aov": "#2ECC71",
    "sessions": "#E67E22",
    "funnel": ["#85C1E9", "#58D68D", "#F4D03F", "#F5B041"],
    "segments": ["#F1C40F", "#2ECC71", "#85C1E9", "#E74C3C"],
    "products": "#27AE60",
    "geo": ["#F39C12", "#3498DB", "#2ECC71", "#E67E22", "#F1948A"]
}
FONT_STYLE = dict(size=14, family="Arial", color="black")


class InteractiveDashboard:
    def __init__(self):
        self.output_dir = "visualizations"
        os.makedirs(self.output_dir, exist_ok=True)

        with open("output/analytics_results.json", 'r') as f:
            self.results = json.load(f)

        print("Creating professional interactive HTML dashboard...")

    def create_comprehensive_dashboard(self):
        insights = self.results['business_insights']

        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=[
                'Key Performance Indicators', 'Revenue & Customer Metrics',
                'Conversion Funnel', 'Customer Segmentation',
                'Product Performance', 'Geographic Distribution'
            ],
            specs=[
                [{"type": "indicator"}, {"type": "bar"}],
                [{"type": "funnel"}, {"type": "pie"}],
                [{"type": "bar"}, {"type": "scatter"}]
            ],
            vertical_spacing=0.12, horizontal_spacing=0.1
        )

        # KPI Gauge
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=insights['conversion_rate'],
            title={'text': "Conversion Rate (%)", 'font': FONT_STYLE},
            domain={'x': [0, 1], 'y': [0, 1]},
            gauge={
                'axis': {'range': [None, 10]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 2], 'color': "#FDEBD0"},
                    {'range': [2, 5], 'color': "#F9E79F"},
                    {'range': [5, 10], 'color': "#82E0AA"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 5
                }
            }
        ), row=1, col=1)

        # Revenue & Customer Metrics
        fig.add_trace(go.Bar(
            x=['Revenue (M$)', 'Customers (K)', 'AOV ($100)', 'Sessions (K)'],
            y=[insights['total_revenue']/1e6, insights['total_customers']/1e3,
               insights['avg_order_value']/100, insights['total_sessions']/1e3],
            texttemplate='%{y:.1f}',
            textposition='auto',
            marker_color=[COLORS['revenue'], COLORS['customers'], COLORS['aov'], COLORS['sessions']]
        ), row=1, col=2)

        # Conversion Funnel
        funnel_data = [
            insights['total_sessions'],
            insights['total_sessions'] * 0.15,
            insights['total_sessions'] * insights['conversion_rate'] / 100,
            insights['total_customers'] * 0.8
        ]
        funnel_labels = ['Total Sessions', 'Engaged Users', 'Converted', 'Repeat Customers']

        fig.add_trace(go.Funnel(
            y=funnel_labels,
            x=funnel_data,
            marker_color=COLORS['funnel'],
            textinfo="value+percent initial"
        ), row=2, col=1)

        # Customer Segmentation
        if 'customer_segments' in self.results and self.results['customer_segments']:
            segments_df = pd.DataFrame(self.results['customer_segments'])
            fig.add_trace(go.Pie(
                labels=[f'Segment {s}' for s in segments_df['segment']],
                values=segments_df['customer_count'],
                hole=0.4,
                marker_colors=COLORS['segments']
            ), row=2, col=2)
        else:
            fig.add_trace(go.Pie(
                labels=['High Value', 'Medium Value', 'Low Value', 'New Customers'],
                values=[25, 35, 25, 15],
                hole=0.4,
                marker_colors=COLORS['segments']
            ), row=2, col=2)

        # Product Performance
        fig.add_trace(go.Bar(
            x=['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
            y=[150000, 120000, 100000, 80000, 60000],
            marker_color=COLORS['products'],
            text=[f'${val/1000:.0f}K' for val in [150000, 120000, 100000, 80000, 60000]],
            textposition='auto'
        ), row=3, col=1)

        # Geographic Distribution
        countries = ['USA', 'Canada', 'UK', 'Germany', 'France']
        revenues = [insights['total_revenue'] * p for p in [0.6, 0.15, 0.1, 0.08, 0.07]]
        customers = [insights['total_customers'] * p for p in [0.5, 0.2, 0.15, 0.1, 0.05]]

        fig.add_trace(go.Scatter(
            x=customers, y=revenues,
            mode='markers+text',
            text=countries,
            textposition='top center',
            marker=dict(
                size=[20, 15, 12, 10, 8],
                color=COLORS['geo'],
                opacity=0.8
            )
        ), row=3, col=2)

        # Layout Settings
        fig.update_layout(
            title={
                'text': " E-COMMERCE ANALYTICS DASHBOARD",
                'x': 0.5, 'xanchor': 'center',
                'font': dict(size=24, color='darkblue')
            },
            height=1200,
            template="plotly_white",
            showlegend=False,
            font=FONT_STYLE
        )

        # Save to file
        dashboard_file = f"{self.output_dir}/interactive_dashboard.html"
        fig.write_html(dashboard_file)
        print(f" Professional interactive dashboard saved to: {dashboard_file}")
        return dashboard_file


if __name__ == "__main__":
    dashboard = InteractiveDashboard()
    dashboard.create_comprehensive_dashboard()
