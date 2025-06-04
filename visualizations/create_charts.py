# visualizations/create_charts.py
"""
Professional Visualization Generator
Creates charts from our analytics results
"""

import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Set professional style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class VisualizationGenerator:
    
    def __init__(self):
        self.output_dir = "visualizations/charts"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Load results
        with open("output/analytics_results.json", 'r') as f:
            self.results = json.load(f)
        
        print(" Creating professional visualizations...")

    def create_business_dashboard(self):
        """Create executive business dashboard"""
        insights = self.results['business_insights']
        
        # Create dashboard
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('E-COMMERCE BUSINESS DASHBOARD', fontsize=20, fontweight='bold')
        
        # Revenue pie chart
        revenue_data = [insights['total_revenue'], 100000000 - insights['total_revenue']]
        labels = [f"Actual Revenue\n${insights['total_revenue']:,.0f}", "Market Potential"]
        colors = ['#2E8B57', '#E6E6FA']
        ax1.pie(revenue_data, labels=labels, autopct='%1.1f%%', startangle=90, colors=colors)
        ax1.set_title('Revenue Achievement', fontweight='bold')
        
        # Customer metrics
        customer_data = [insights['total_customers'], insights['active_products']]
        x_pos = ['Active Customers', 'Active Products']
        bars = ax2.bar(x_pos, customer_data, color=['skyblue', 'lightcoral'])
        ax2.set_title('Customer & Product Metrics', fontweight='bold')
        ax2.set_ylabel('Count')
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{customer_data[i]:,}', ha='center', va='bottom', fontweight='bold')
        
        # Conversion funnel
        funnel_data = [insights['total_sessions'], 
                      insights['total_sessions'] * insights['conversion_rate'] / 100,
                      insights['total_customers']]
        funnel_labels = ['Total Sessions', 'Converted Sessions', 'Active Customers']
        ax3.bar(funnel_labels, funnel_data, color=['lightblue', 'orange', 'green'])
        ax3.set_title('Conversion Funnel', fontweight='bold')
        ax3.set_ylabel('Count')
        ax3.tick_params(axis='x', rotation=45)
        
        # Key KPIs
        kpis = ['Conversion Rate', 'Avg Order Value', 'Revenue (Millions)']
        kpi_values = [insights['conversion_rate'], insights['avg_order_value'], insights['total_revenue']/1000000]
        bars = ax4.bar(kpis, kpi_values, color=['gold', 'lightgreen', 'salmon'])
        ax4.set_title('Key Performance Indicators', fontweight='bold')
        ax4.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/business_dashboard.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(" Business dashboard created")

    def create_customer_segments_chart(self):
        """Create customer segmentation chart"""
        if 'customer_segments' not in self.results:
            print("⚠️ No customer segment data available")
            return
            
        segments_df = pd.DataFrame(self.results['customer_segments'])
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        fig.suptitle('CUSTOMER SEGMENTATION ANALYSIS', fontsize=16, fontweight='bold')
        
        # Customer count by segment
        ax1.bar(segments_df['segment'], segments_df['customer_count'], 
                color='lightblue', alpha=0.8)
        ax1.set_title('Customers per Segment')
        ax1.set_ylabel('Customer Count')
        ax1.tick_params(axis='x', rotation=45)
        
        # Average monetary value by segment
        ax2.bar(segments_df['segment'], segments_df['avg_monetary'], 
                color='lightgreen', alpha=0.8)
        ax2.set_title('Average Spend per Segment')
        ax2.set_ylabel('Average Monetary Value ($)')
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/customer_segments.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(" Customer segmentation chart created")

    def create_executive_summary(self):
        """Create executive summary infographic"""
        insights = self.results['business_insights']
        
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        ax.axis('off')
        fig.patch.set_facecolor('white')
        
        # Title
        ax.text(5, 9.5, 'E-COMMERCE ANALYTICS EXECUTIVE SUMMARY', 
                fontsize=18, fontweight='bold', ha='center')
        
        # Subtitle
        ax.text(5, 9, 'Multi-Database Architecture: MongoDB + HBase + Spark', 
                fontsize=12, ha='center', style='italic')
        
        # Key metrics boxes
        metrics = [
            (f"${insights['total_revenue']/1000000:.1f}M", "Total Revenue", 2, 7.5),
            (f"{insights['total_customers']:,}", "Active Customers", 5, 7.5),
            (f"{insights['conversion_rate']:.1f}%", "Conversion Rate", 8, 7.5),
            (f"${insights['avg_order_value']:.0f}", "Avg Order Value", 2, 5.5),
            (f"{insights['total_sessions']:,}", "Sessions Analyzed", 5, 5.5),
            (f"{insights['active_products']:,}", "Active Products", 8, 5.5)
        ]
        
        for value, label, x, y in metrics:
            # Create metric box
            rect = plt.Rectangle((x-0.8, y-0.8), 1.6, 1.6, 
                               facecolor='lightblue', alpha=0.3, edgecolor='navy')
            ax.add_patch(rect)
            ax.text(x, y+0.3, value, fontsize=14, fontweight='bold', ha='center')
            ax.text(x, y-0.3, label, fontsize=10, ha='center')
        
        # Architecture section
        ax.text(5, 3.5, 'TECHNICAL ARCHITECTURE', fontsize=14, fontweight='bold', ha='center')
        
        # Database boxes
        databases = [
            ("MongoDB", "Document Store\nUsers, Products\nTransactions", 2, 2),
            ("HBase", "Time-Series\nSession Data\nUser Behavior", 5, 2),
            ("Spark", "Distributed\nProcessing\nML Analytics", 8, 2)
        ]
        
        for db_name, description, x, y in databases:
            rect = plt.Rectangle((x-0.9, y-0.7), 1.8, 1.4, 
                               facecolor='lightgreen', alpha=0.3, edgecolor='darkgreen')
            ax.add_patch(rect)
            ax.text(x, y+0.3, db_name, fontsize=12, fontweight='bold', ha='center')
            ax.text(x, y-0.2, description, fontsize=8, ha='center')
        
        # Footer
        ax.text(5, 0.5, f'Generated: {datetime.now().strftime("%Y-%m-%d")} | AUCA Big Data Analytics Final Project', 
                fontsize=10, ha='center', style='italic')
        
        plt.savefig(f'{self.output_dir}/executive_summary.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(" Executive summary created")

    def generate_all_charts(self):
        """Generate all visualizations"""
        print("🎨 Generating professional visualizations...")
        
        self.create_business_dashboard()
        self.create_customer_segments_chart()
        self.create_executive_summary()
        
        print("\n" + "="*60)
        print(" ALL VISUALIZATIONS COMPLETED!")
        print("="*60)
        print(f" Charts saved to: {self.output_dir}/")
        print(" Created visualizations:")
        print("  • business_dashboard.png")
        print("  • customer_segments.png")
        print("  • executive_summary.png")
        print("="*60)


if __name__ == "__main__":
    generator = VisualizationGenerator()
    generator.generate_all_charts()
