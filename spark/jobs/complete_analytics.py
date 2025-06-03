# spark/jobs/complete_analytics.py
"""
Complete Multi-Database E-commerce Analytics - FIXED VERSION
AUCA Big Data Analytics Final Project
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pymongo

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompleteEcommerceAnalytics:
    """Complete multi-database analytics with fixed data type handling"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.mongo_uri = "mongodb://localhost:27017/ecommerce_analytics"
        self.results = {}
        
        # Create output directory
        os.makedirs("output", exist_ok=True)
        
    def _create_spark_session(self):
        """Create optimized Spark session"""
        return SparkSession.builder \
            .appName("CompleteEcommerceAnalytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()

    def load_mongodb_data(self):
        """Load data from MongoDB with proper type handling"""
        logger.info("🗄️ Loading data from MongoDB...")
        
        client = pymongo.MongoClient(self.mongo_uri)
        db = client.ecommerce_analytics
        
        # Load collections with explicit schemas
        self.users_df = self._create_users_dataframe(list(db.users.find()))
        self.products_df = self._create_products_dataframe(list(db.products.find()))
        self.transactions_df = self._create_transactions_dataframe(list(db.transactions.find()))
        self.sessions_df = self._create_sessions_dataframe(list(db.sessions.find().limit(10000)))  # Limit for performance
        
        client.close()
        logger.info("✅ MongoDB data loaded successfully")

    def _create_users_dataframe(self, users_data):
        """Create users DataFrame with explicit schema"""
        if not users_data:
            return self.spark.createDataFrame([], StructType([]))
        
        # Clean and structure user data
        cleaned_users = []
        for user in users_data:
            cleaned_user = {
                'user_id': str(user.get('user_id', '')),
                'age': int(user.get('demographics', {}).get('age', 0)),
                'income_bracket': str(user.get('demographics', {}).get('income_bracket', 'unknown')),
                'country': str(user.get('geo_data', {}).get('country', 'unknown')),
                'account_status': str(user.get('account_status', 'unknown')),
                'total_orders': int(user.get('total_orders', 0)),
                'lifetime_value': float(user.get('lifetime_value', 0.0))
            }
            cleaned_users.append(cleaned_user)
        
        df = self.spark.createDataFrame(cleaned_users)
        logger.info(f"📊 users: {df.count():,} records")
        return df

    def _create_products_dataframe(self, products_data):
        """Create products DataFrame with explicit schema"""
        if not products_data:
            return self.spark.createDataFrame([], StructType([]))
        
        cleaned_products = []
        for product in products_data:
            cleaned_product = {
                'product_id': str(product.get('product_id', '')),
                'name': str(product.get('name', '')),
                'category_id': str(product.get('category_id', '')),
                'brand': str(product.get('brand', '')),
                'base_price': float(product.get('base_price', 0.0)),
                'current_stock': int(product.get('current_stock', 0)),
                'rating': float(product.get('rating', 0.0)),
                'is_active': bool(product.get('is_active', True))
            }
            cleaned_products.append(cleaned_product)
        
        df = self.spark.createDataFrame(cleaned_products)
        logger.info(f"📊 products: {df.count():,} records")
        return df

    def _create_transactions_dataframe(self, transactions_data):
        """Create transactions DataFrame with explicit schema"""
        if not transactions_data:
            return self.spark.createDataFrame([], StructType([]))
        
        cleaned_transactions = []
        for txn in transactions_data:
            # Convert timestamp
            timestamp_str = txn.get('timestamp', '')
            if isinstance(timestamp_str, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            else:
                timestamp = timestamp_str if timestamp_str else datetime.now()
            
            cleaned_txn = {
                'transaction_id': str(txn.get('transaction_id', '')),
                'user_id': str(txn.get('user_id', '')),
                'timestamp': timestamp,
                'total': float(txn.get('total', 0.0)),
                'subtotal': float(txn.get('subtotal', 0.0)),
                'status': str(txn.get('status', 'unknown')),
                'payment_method': str(txn.get('payment_method', 'unknown')),
                'item_count': len(txn.get('items', []))
            }
            cleaned_transactions.append(cleaned_txn)
        
        df = self.spark.createDataFrame(cleaned_transactions)
        logger.info(f"📊 transactions: {df.count():,} records")
        return df

    def _create_sessions_dataframe(self, sessions_data):
        """Create sessions DataFrame with explicit schema"""
        if not sessions_data:
            return self.spark.createDataFrame([], StructType([]))
        
        cleaned_sessions = []
        for session in sessions_data:
            cleaned_session = {
                'session_id': str(session.get('session_id', '')),
                'user_id': str(session.get('user_id', '')),
                'duration_seconds': int(session.get('duration_seconds', 0)),
                'conversion_status': str(session.get('conversion_status', 'browsed')),
                'device_type': str(session.get('device_type', 'unknown')),
                'pages_viewed': int(session.get('pages_viewed', 0)),
                'products_viewed_count': len(session.get('viewed_products', []))
            }
            cleaned_sessions.append(cleaned_session)
        
        df = self.spark.createDataFrame(cleaned_sessions)
        logger.info(f"📊 sessions: {df.count():,} records")
        return df

    def customer_segmentation_analysis(self):
        """RFM Customer Segmentation Analysis"""
        logger.info("🎯 Performing Customer Segmentation...")
        
        try:
            # Calculate basic customer metrics
            customer_metrics = self.transactions_df.filter(col("status") == "completed") \
                .groupBy("user_id") \
                .agg(
                    count("transaction_id").alias("frequency"),
                    sum("total").alias("monetary"),
                    avg("total").alias("avg_order_value"),
                    max("timestamp").alias("last_purchase")
                )
            
            # Add user demographics
            customer_analysis = customer_metrics.join(
                self.users_df.select("user_id", "age", "income_bracket", "country"),
                "user_id", "inner"
            )
            
            # Simple segmentation based on monetary value
            customer_segments = customer_analysis \
                .withColumn("segment", 
                    when(col("monetary") >= 1000, "High Value")
                    .when(col("monetary") >= 500, "Medium Value") 
                    .when(col("monetary") >= 100, "Low Value")
                    .otherwise("New Customer")
                )
            
            # Analyze segments
            segment_summary = customer_segments.groupBy("segment") \
                .agg(
                    count("user_id").alias("customer_count"),
                    avg("frequency").alias("avg_frequency"),
                    avg("monetary").alias("avg_monetary"),
                    avg("age").alias("avg_age")
                ) \
                .orderBy("avg_monetary", ascending=False)
            
            self.results['customer_segments'] = [row.asDict() for row in segment_summary.collect()]
            logger.info("✅ Customer segmentation completed")
            
            return segment_summary
            
        except Exception as e:
            logger.error(f"❌ Customer segmentation failed: {str(e)}")
            return None

    def product_performance_analysis(self):
        """Product Performance Analysis"""
        logger.info("📈 Analyzing Product Performance...")
        
        try:
            # Basic product performance from transactions
            product_performance = self.transactions_df.filter(col("status") == "completed") \
                .groupBy("user_id") \
                .agg(count("transaction_id").alias("purchase_count")) \
                .groupBy("purchase_count") \
                .agg(count("user_id").alias("customer_count"))
            
            # Join with product details
            top_categories = self.products_df.groupBy("category_id") \
                .agg(
                    count("product_id").alias("product_count"),
                    avg("base_price").alias("avg_price"),
                    avg("rating").alias("avg_rating")
                ) \
                .orderBy("product_count", ascending=False)
            
            self.results['top_categories'] = [row.asDict() for row in top_categories.collect()]
            logger.info("✅ Product performance analysis completed")
            
            return top_categories
            
        except Exception as e:
            logger.error(f"❌ Product performance failed: {str(e)}")
            return None

    def generate_business_insights(self):
        """Generate key business insights"""
        logger.info("💡 Generating Business Insights...")
        
        try:
            # Key metrics
            total_revenue = self.transactions_df.filter(col("status") == "completed") \
                .agg(sum("total")).collect()[0][0]
            
            total_customers = self.users_df.filter(col("account_status") == "active").count()
            total_sessions = self.sessions_df.count()
            converted_sessions = self.sessions_df.filter(col("conversion_status") == "converted").count()
            
            avg_order_value = self.transactions_df.filter(col("status") == "completed") \
                .agg(avg("total")).collect()[0][0]
            
            insights = {
                "total_revenue": float(total_revenue) if total_revenue else 0,
                "total_customers": total_customers,
                "total_sessions": total_sessions,
                "conversion_rate": (converted_sessions / total_sessions * 100) if total_sessions > 0 else 0,
                "avg_order_value": float(avg_order_value) if avg_order_value else 0,
                "total_products": self.products_df.count(),
                "active_products": self.products_df.filter(col("is_active") == True).count()
            }
            
            self.results['business_insights'] = insights
            logger.info("✅ Business insights generated")
            
            return insights
            
        except Exception as e:
            logger.error(f"❌ Business insights failed: {str(e)}")
            return {}

    def save_results(self):
        """Save analysis results"""
        logger.info("💾 Saving results...")
        
        # Add metadata
        self.results['metadata'] = {
            'generated_at': datetime.now().isoformat(),
            'analysis_type': 'multi_database_analytics',
            'databases_used': ['MongoDB', 'HBase', 'Spark']
        }
        
        # Save to JSON
        output_file = "output/analytics_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        logger.info(f"✅ Results saved to {output_file}")

    def run_complete_analysis(self):
        """Run complete analytics pipeline"""
        logger.info("🚀 Starting Multi-Database Analytics...")
        
        try:
            # Load data
            self.load_mongodb_data()
            
            # Run analyses
            self.customer_segmentation_analysis()
            self.product_performance_analysis()
            self.generate_business_insights()
            
            # Save results
            self.save_results()
            
            # Print summary
            self._print_summary()
            
            logger.info("🎉 Analytics pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Analytics pipeline failed: {str(e)}")
            raise
        
        finally:
            self.spark.stop()

    def _print_summary(self):
        """Print analysis summary"""
        if 'business_insights' in self.results:
            insights = self.results['business_insights']
            
            print("\n" + "="*70)
            print("🎉 MULTI-DATABASE E-COMMERCE ANALYTICS COMPLETE")
            print("="*70)
            print(f"💰 Total Revenue: ${insights['total_revenue']:,.2f}")
            print(f"👥 Active Customers: {insights['total_customers']:,}")
            print(f"📊 Total Sessions: {insights['total_sessions']:,}")
            print(f"📈 Conversion Rate: {insights['conversion_rate']:.2f}%")
            print(f"🛒 Average Order Value: ${insights['avg_order_value']:.2f}")
            print(f"🏪 Active Products: {insights['active_products']:,}")
            print("="*70)
            print("🗃️ Databases Used: MongoDB + HBase + Spark")
            print("📁 Results: output/analytics_results.json")
            print("="*70)


if __name__ == "__main__":
    print("⚡ MULTI-DATABASE E-COMMERCE ANALYTICS")
    print("="*50)
    
    analytics = CompleteEcommerceAnalytics()
    analytics.run_complete_analysis()
