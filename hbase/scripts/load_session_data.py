# hbase/scripts/load_session_data.py
"""
HBase Session Data Loader
AUCA Big Data Analytics Final Project
Load session data from JSON files into HBase tables
"""

import json
import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Any
import happybase
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hbase/logs/data_loading.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HBaseSessionLoader:
    """Load session data into HBase for time-series analytics"""
    
    def __init__(self, host='localhost', port=9090):
        self.host = host
        self.port = port
        self.connection = None
        
        # Ensure log directory exists
        os.makedirs("hbase/logs", exist_ok=True)
        
        logger.info("HBase Session Loader initialized")

    def connect(self) -> bool:
        """Connect to HBase via Thrift"""
        try:
            self.connection = happybase.Connection(
                host=self.host,
                port=self.port,
                timeout=30000,
                autoconnect=True
            )
            
            # Test connection by listing tables
            tables = self.connection.tables()
            logger.info(f" Connected to HBase. Available tables: {[t.decode() for t in tables]}")
            return True
            
        except Exception as e:
            logger.error(f" Failed to connect to HBase: {str(e)}")
            logger.info("💡 Make sure HBase Thrift server is running on localhost:9090")
            return False

    def verify_tables(self) -> bool:
        """Verify required tables exist"""
        required_tables = [b'user_sessions', b'product_views', b'user_events']
        
        try:
            existing_tables = self.connection.tables()
            
            missing_tables = []
            for table in required_tables:
                if table not in existing_tables:
                    missing_tables.append(table.decode())
            
            if missing_tables:
                logger.error(f" Missing tables: {missing_tables}")
                return False
            
            logger.info(" All required HBase tables exist")
            return True
            
        except Exception as e:
            logger.error(f" Error verifying tables: {str(e)}")
            return False

    def load_session_files(self, data_dir="data/raw"):
        """Load all session files into HBase"""
        logger.info(" Starting session data loading into HBase...")
        
        # Find all session files
        session_files = []
        for i in range(20):  # We know we have sessions_000.json to sessions_019.json
            file_path = os.path.join(data_dir, f"sessions_{i:03d}.json")
            if os.path.exists(file_path):
                session_files.append(file_path)
        
        if not session_files:
            logger.error(f" No session files found in {data_dir}")
            return False
        
        logger.info(f" Found {len(session_files)} session files to load")
        
        total_sessions_loaded = 0
        
        # Get HBase table
        sessions_table = self.connection.table('user_sessions')
        
        for file_path in session_files:
            try:
                logger.info(f" Loading {os.path.basename(file_path)}...")
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    sessions_data = json.load(f)
                
                if not sessions_data:
                    logger.warning(f"⚠️  No data in {file_path}")
                    continue
                
                # Process sessions in batches
                batch_size = 1000
                batch_data = []
                
                for session in tqdm(sessions_data, desc=f"Processing {os.path.basename(file_path)}"):
                    row_key, row_data = self._convert_session_to_hbase_format(session)
                    batch_data.append((row_key, row_data))
                    
                    # Write batch when it reaches batch_size
                    if len(batch_data) >= batch_size:
                        self._write_batch_to_hbase(sessions_table, batch_data)
                        total_sessions_loaded += len(batch_data)
                        batch_data = []
                
                # Write remaining data
                if batch_data:
                    self._write_batch_to_hbase(sessions_table, batch_data)
                    total_sessions_loaded += len(batch_data)
                
                logger.info(f" Loaded {len(sessions_data):,} sessions from {os.path.basename(file_path)}")
                
            except Exception as e:
                logger.error(f" Failed to load {file_path}: {str(e)}")
                continue
        
        logger.info(f" Total sessions loaded into HBase: {total_sessions_loaded:,}")
        return total_sessions_loaded > 0

    def _convert_session_to_hbase_format(self, session: Dict) -> tuple:
        """Convert session JSON to HBase row format"""
        # Create row key: user_id + timestamp (for time-series queries)
        user_id = session.get('user_id', 'unknown')
        start_time = session.get('start_time', '')
        
        # Create sortable timestamp for row key
        try:
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            timestamp_str = dt.strftime('%Y%m%d_%H%M%S')
        except:
            timestamp_str = '00000000_000000'
        
        row_key = f"{user_id}_{timestamp_str}_{session.get('session_id', 'unknown')}"
        
        # Prepare column family data
        row_data = {}
        
        # Session info column family
        row_data[b'session_info:session_id'] = str(session.get('session_id', '')).encode('utf-8')
        row_data[b'session_info:user_id'] = str(session.get('user_id', '')).encode('utf-8')
        row_data[b'session_info:start_time'] = str(session.get('start_time', '')).encode('utf-8')
        row_data[b'session_info:end_time'] = str(session.get('end_time', '')).encode('utf-8')
        row_data[b'session_info:duration_seconds'] = str(session.get('duration_seconds', 0)).encode('utf-8')
        row_data[b'session_info:conversion_status'] = str(session.get('conversion_status', 'browsed')).encode('utf-8')
        row_data[b'session_info:referrer'] = str(session.get('referrer', 'direct')).encode('utf-8')
        
        # Page views data
        page_views = session.get('page_views', [])
        row_data[b'page_views:page_count'] = str(len(page_views)).encode('utf-8')
        
        if page_views:
            # Store first and last page info
            row_data[b'page_views:first_page'] = str(page_views[0].get('page_type', '')).encode('utf-8')
            row_data[b'page_views:last_page'] = str(page_views[-1].get('page_type', '')).encode('utf-8')
            
            # Store page view summary as JSON
            page_summary = [
                {
                    'page_type': pv.get('page_type'),
                    'view_duration': pv.get('view_duration'),
                    'product_id': pv.get('product_id'),
                    'category_id': pv.get('category_id')
                }
                for pv in page_views[:10]  # Limit to first 10 page views
            ]
            row_data[b'page_views:page_summary'] = json.dumps(page_summary).encode('utf-8')
        
        # Device data
        device_profile = session.get('device_profile', {})
        row_data[b'device_data:device_type'] = str(device_profile.get('type', 'unknown')).encode('utf-8')
        row_data[b'device_data:browser'] = str(device_profile.get('browser', 'unknown')).encode('utf-8')
        row_data[b'device_data:os'] = str(device_profile.get('os', 'unknown')).encode('utf-8')
        
        # Geographic data
        geo_data = session.get('geo_data', {})
        row_data[b'device_data:country'] = str(geo_data.get('country', 'unknown')).encode('utf-8')
        row_data[b'device_data:state'] = str(geo_data.get('state', 'unknown')).encode('utf-8')
        row_data[b'device_data:city'] = str(geo_data.get('city', 'unknown')).encode('utf-8')
        
        # Conversion data
        viewed_products = session.get('viewed_products', [])
        row_data[b'conversion_data:products_viewed'] = str(len(viewed_products)).encode('utf-8')
        
        if viewed_products:
            row_data[b'conversion_data:product_list'] = json.dumps(viewed_products).encode('utf-8')
        
        # Cart data
        cart_contents = session.get('cart_contents', {})
        row_data[b'conversion_data:cart_items'] = str(len(cart_contents)).encode('utf-8')
        
        if cart_contents:
            cart_total = sum(item.get('quantity', 0) * item.get('unit_price', 0) 
                           for item in cart_contents.values())
            row_data[b'conversion_data:cart_value'] = str(cart_total).encode('utf-8')
            row_data[b'conversion_data:cart_data'] = json.dumps(cart_contents).encode('utf-8')
        
        return row_key.encode('utf-8'), row_data

    def _write_batch_to_hbase(self, table, batch_data: List[tuple]):
        """Write batch of data to HBase"""
        try:
            # Create batch
            batch = table.batch()
            
            for row_key, row_data in batch_data:
                batch.put(row_key, row_data)
            
            # Send batch
            batch.send()
            
        except Exception as e:
            logger.error(f" Failed to write batch to HBase: {str(e)}")
            raise

    def load_product_interaction_data(self):
        """Load product interaction data from sessions into product_views table"""
        logger.info(" Loading product interaction data...")
        
        try:
            # Read session data and extract product interactions
            interactions = {}
            
            # Process session files to extract product views
            for i in range(5):  # Process first 5 session files for demo
                file_path = f"data/raw/sessions_{i:03d}.json"
                if not os.path.exists(file_path):
                    continue
                    
                with open(file_path, 'r') as f:
                    sessions_data = json.load(f)
                
                for session in sessions_data:
                    for product_id in session.get('viewed_products', []):
                        if product_id not in interactions:
                            interactions[product_id] = {
                                'view_count': 0,
                                'total_duration': 0,
                                'unique_users': set(),
                                'conversion_count': 0
                            }
                        
                        interactions[product_id]['view_count'] += 1
                        interactions[product_id]['unique_users'].add(session['user_id'])
                        
                        if session.get('conversion_status') == 'converted':
                            interactions[product_id]['conversion_count'] += 1
            
            # Load into product_views table
            product_table = self.connection.table('product_views')
            batch = product_table.batch()
            
            for product_id, data in interactions.items():
                row_key = f"product_{product_id}_{datetime.now().strftime('%Y%m%d')}"
                
                row_data = {
                    b'view_metrics:view_count': str(data['view_count']).encode('utf-8'),
                    b'view_metrics:unique_users': str(len(data['unique_users'])).encode('utf-8'),
                    b'view_metrics:conversion_count': str(data['conversion_count']).encode('utf-8'),
                    b'interaction_data:last_updated': datetime.now().isoformat().encode('utf-8')
                }
                
                batch.put(row_key.encode('utf-8'), row_data)
            
            batch.send()
            logger.info(f" Loaded product interaction data for {len(interactions)} products")
            
        except Exception as e:
            logger.error(f" Failed to load product interaction data: {str(e)}")

    def verify_data_loading(self):
        """Verify data was loaded correctly"""
        logger.info("Verifying HBase data loading...")
        
        try:
            # Check user_sessions table
            sessions_table = self.connection.table('user_sessions')
            session_count = 0
            
            # Scan first 100 rows as verification
            for key, data in sessions_table.scan(limit=100):
                session_count += 1
            
            logger.info(f" Verified: {session_count} session records accessible")
            
            # Check product_views table
            product_table = self.connection.table('product_views')
            product_count = 0
            
            for key, data in product_table.scan(limit=10):
                product_count += 1
            
            logger.info(f" Verified: {product_count} product interaction records")
            
            return session_count > 0
            
        except Exception as e:
            logger.error(f" Verification failed: {str(e)}")
            return False

    def run_complete_loading(self):
        """Run complete data loading process"""
        logger.info(" Starting complete HBase data loading process...")
        
        try:
            # Connect to HBase
            if not self.connect():
                return False
            
            # Verify tables exist
            if not self.verify_tables():
                return False
            
            # Load session data
            if not self.load_session_files():
                return False
            
            # Load product interaction data
            self.load_product_interaction_data()
            
            # Verify loading
            if not self.verify_data_loading():
                return False
            
            logger.info("=" * 60)
            logger.info(" HBASE DATA LOADING COMPLETE!")
            logger.info("=" * 60)
            logger.info(" Session data loaded into user_sessions table")
            logger.info(" Product interactions loaded into product_views table")
            logger.info("Data verified and accessible")
            logger.info("Access HBase Master UI: http://localhost:16010")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f" Data loading failed: {str(e)}")
            return False
        
        finally:
            if self.connection:
                self.connection.close()
                logger.info("🔌 HBase connection closed")


if __name__ == "__main__":
    print("  HBASE SESSION DATA LOADER")
    print("=" * 50)
    
    loader = HBaseSessionLoader()
    success = loader.run_complete_loading()
    
    if success:
        print("\n HBase data loading completed successfully!")
        print(" Access HBase Master UI: http://localhost:16010")
        print(" Session data ready for time-series analytics!")
    else:
        print("\n HBase data loading failed. Check logs for details.")
        sys.exit(1)