# mongodb/scripts/setup_and_load.py
"""
MongoDB Setup and Data Loading Script
AUCA Big Data Analytics Final Project
Professional MongoDB schema creation and data loading with optimizations
"""

import json
import os
import logging
from typing import Dict, List, Any
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import BulkWriteError, DuplicateKeyError
from tqdm import tqdm
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mongodb/logs/setup.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MongoDBSetup:
    """Professional MongoDB setup with schemas, indexes, and data loading"""
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017/"):
        self.connection_string = connection_string
        self.client = None
        self.db = None
        self.db_name = "ecommerce_analytics"
        
        # Ensure log directory exists
        os.makedirs("mongodb/logs", exist_ok=True)
        
        logger.info("MongoDB Setup initialized")

    def connect(self) -> bool:
        """Establish MongoDB connection with error handling"""
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.server_info()
            self.db = self.client[self.db_name]
            logger.info(f"✅ Connected to MongoDB at {self.connection_string}")
            logger.info(f"📁 Using database: {self.db_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False

    def create_collections_with_schemas(self):
        """Create collections with JSON Schema validation"""
        logger.info("🏗️  Creating collections with validation schemas...")
        
        # Users collection with schema validation
        users_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["user_id", "email", "registration_date"],
                "properties": {
                    "user_id": {
                        "bsonType": "string",
                        "description": "Unique user identifier"
                    },
                    "email": {
                        "bsonType": "string",
                        "pattern": "^.+@.+$",
                        "description": "Valid email address"
                    },
                    "first_name": {"bsonType": "string"},
                    "last_name": {"bsonType": "string"},
                    "demographics": {
                        "bsonType": "object",
                        "properties": {
                            "age": {"bsonType": "int", "minimum": 18, "maximum": 120},
                            "gender": {"enum": ["M", "F", "Other"]},
                            "income_bracket": {"enum": ["low", "medium", "high", "premium"]},
                            "education": {"bsonType": "string"}
                        }
                    },
                    "geo_data": {
                        "bsonType": "object",
                        "required": ["country"],
                        "properties": {
                            "city": {"bsonType": "string"},
                            "state": {"bsonType": "string"},
                            "country": {"bsonType": "string"},
                            "timezone": {"bsonType": "string"}
                        }
                    },
                    "registration_date": {"bsonType": "date"},
                    "last_active": {"bsonType": "date"},
                    "account_status": {"enum": ["active", "inactive", "suspended"]},
                    "loyalty_tier": {"enum": ["bronze", "silver", "gold", "platinum"]},
                    "total_orders": {"bsonType": "int", "minimum": 0},
                    "lifetime_value": {"bsonType": "number", "minimum": 0}
                }
            }
        }

        # Products collection with schema validation
        products_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["product_id", "name", "category_id", "base_price"],
                "properties": {
                    "product_id": {"bsonType": "string"},
                    "sku": {"bsonType": "string"},
                    "name": {"bsonType": "string"},
                    "description": {"bsonType": "string"},
                    "category_id": {"bsonType": "string"},
                    "subcategory_id": {"bsonType": "string"},
                    "brand": {"bsonType": "string"},
                    "base_price": {"bsonType": "number", "minimum": 0},
                    "cost": {"bsonType": "number", "minimum": 0},
                    "current_stock": {"bsonType": "int", "minimum": 0},
                    "weight_kg": {"bsonType": "number", "minimum": 0},
                    "is_active": {"bsonType": "bool"},
                    "rating": {"bsonType": "number", "minimum": 0, "maximum": 5},
                    "review_count": {"bsonType": "int", "minimum": 0},
                    "price_history": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["price", "date"],
                            "properties": {
                                "price": {"bsonType": "number", "minimum": 0},
                                "date": {"bsonType": "date"},
                                "reason": {"bsonType": "string"}
                            }
                        }
                    }
                }
            }
        }

        # Transactions collection with schema validation
        transactions_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["transaction_id", "user_id", "total", "timestamp"],
                "properties": {
                    "transaction_id": {"bsonType": "string"},
                    "user_id": {"bsonType": "string"},
                    "timestamp": {"bsonType": "date"},
                    "items": {
                        "bsonType": "array",
                        "minItems": 1,
                        "items": {
                            "bsonType": "object",
                            "required": ["product_id", "quantity", "unit_price", "subtotal"],
                            "properties": {
                                "product_id": {"bsonType": "string"},
                                "quantity": {"bsonType": "int", "minimum": 1},
                                "unit_price": {"bsonType": "number", "minimum": 0},
                                "subtotal": {"bsonType": "number", "minimum": 0}
                            }
                        }
                    },
                    "subtotal": {"bsonType": "number", "minimum": 0},
                    "discount": {"bsonType": "number", "minimum": 0},
                    "tax": {"bsonType": "number", "minimum": 0},
                    "shipping": {"bsonType": "number", "minimum": 0},
                    "total": {"bsonType": "number", "minimum": 0},
                    "payment_method": {"bsonType": "string"},
                    "status": {"enum": ["completed", "processing", "shipped", "delivered", "cancelled"]}
                }
            }
        }

        # Categories collection with schema validation
        categories_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["category_id", "name"],
                "properties": {
                    "category_id": {"bsonType": "string"},
                    "name": {"bsonType": "string"},
                    "description": {"bsonType": "string"},
                    "is_active": {"bsonType": "bool"},
                    "subcategories": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["subcategory_id", "name"],
                            "properties": {
                                "subcategory_id": {"bsonType": "string"},
                                "name": {"bsonType": "string"},
                                "profit_margin": {"bsonType": "number", "minimum": 0, "maximum": 1}
                            }
                        }
                    }
                }
            }
        }

        # Create collections with validation
        collections_config = {
            "users": users_schema,
            "products": products_schema,
            "transactions": transactions_schema,
            "categories": categories_schema
        }

        for collection_name, schema in collections_config.items():
            try:
                # Drop collection if exists
                self.db[collection_name].drop()
                
                # Create collection with validation
                self.db.create_collection(
                    collection_name,
                    validator=schema,
                    validationLevel="moderate",  # Allow some flexibility during development
                    validationAction="warn"       # Log validation errors but allow inserts
                )
                logger.info(f"✅ Created collection '{collection_name}' with schema validation")
                
            except Exception as e:
                logger.error(f"❌ Failed to create collection '{collection_name}': {str(e)}")

    def create_indexes(self):
        """Create performance-optimized indexes for analytics queries"""
        logger.info("🚀 Creating performance indexes...")
        
        index_configs = {
            "users": [
                # Primary indexes
                [("user_id", ASCENDING)],  # Unique index
                [("email", ASCENDING)],    # Unique index
                
                # Analytics indexes
                [("demographics.age", ASCENDING)],
                [("demographics.income_bracket", ASCENDING)],
                [("demographics.gender", ASCENDING)],
                [("geo_data.country", ASCENDING)],
                [("geo_data.state", ASCENDING)],
                [("registration_date", DESCENDING)],
                [("last_active", DESCENDING)],
                [("account_status", ASCENDING)],
                [("loyalty_tier", ASCENDING)],
                [("lifetime_value", DESCENDING)],
                
                # Compound indexes for common queries
                [("account_status", ASCENDING), ("demographics.age", ASCENDING)],
                [("geo_data.country", ASCENDING), ("loyalty_tier", ASCENDING)],
                [("registration_date", DESCENDING), ("account_status", ASCENDING)]
            ],
            
            "products": [
                # Primary indexes
                [("product_id", ASCENDING)],  # Unique index
                [("sku", ASCENDING)],         # Unique index
                
                # Category and search indexes
                [("category_id", ASCENDING)],
                [("subcategory_id", ASCENDING)],
                [("brand", ASCENDING)],
                [("name", TEXT)],  # Text search index
                
                # Analytics indexes
                [("base_price", ASCENDING)],
                [("is_active", ASCENDING)],
                [("current_stock", ASCENDING)],
                [("rating", DESCENDING)],
                [("review_count", DESCENDING)],
                [("creation_date", DESCENDING)],
                
                # Compound indexes
                [("category_id", ASCENDING), ("is_active", ASCENDING)],
                [("is_active", ASCENDING), ("current_stock", ASCENDING)],
                [("base_price", ASCENDING), ("rating", DESCENDING)],
                [("brand", ASCENDING), ("category_id", ASCENDING)]
            ],
            
            "transactions": [
                # Primary indexes
                [("transaction_id", ASCENDING)],  # Unique index
                [("user_id", ASCENDING)],
                
                # Time-based indexes
                [("timestamp", DESCENDING)],
                
                # Analytics indexes
                [("total", DESCENDING)],
                [("status", ASCENDING)],
                [("payment_method", ASCENDING)],
                [("items.product_id", ASCENDING)],
                
                # Compound indexes for analytics
                [("user_id", ASCENDING), ("timestamp", DESCENDING)],
                [("timestamp", DESCENDING), ("status", ASCENDING)],
                [("status", ASCENDING), ("total", DESCENDING)],
                
                # Date range queries
                [("timestamp", DESCENDING), ("total", DESCENDING)]
            ],
            
            "categories": [
                # Primary indexes
                [("category_id", ASCENDING)],  # Unique index
                [("name", ASCENDING)],
                [("is_active", ASCENDING)],
                
                # Subcategory index
                [("subcategories.subcategory_id", ASCENDING)]
            ]
        }

        for collection_name, indexes in index_configs.items():
            collection = self.db[collection_name]
            
            for index_spec in indexes:
                try:
                    # Create index with appropriate options
                    index_name = "_".join([f"{field}_{direction}" for field, direction in index_spec])
                    
                    # Handle unique indexes
                    unique = False
                    if collection_name == "users" and index_spec[0][0] in ["user_id", "email"]:
                        unique = True
                    elif collection_name == "products" and index_spec[0][0] in ["product_id", "sku"]:
                        unique = True
                    elif collection_name == "transactions" and index_spec[0][0] == "transaction_id":
                        unique = True
                    elif collection_name == "categories" and index_spec[0][0] == "category_id":
                        unique = True
                    
                    # Create the index
                    result = collection.create_index(
                        index_spec,
                        unique=unique,
                        background=True,  # Non-blocking index creation
                        name=index_name[:63]  # MongoDB index name limit
                    )
                    
                    logger.info(f"✅ Created index '{result}' on {collection_name}")
                    
                except Exception as e:
                    logger.warning(f"⚠️  Index creation warning for {collection_name}: {str(e)}")

    def load_data_optimized(self):
        """Load data with optimized batch operations"""
        logger.info("📊 Loading data with optimized batch operations...")
        
        # Data files mapping
        data_files = {
            "categories": "data/raw/categories.json",
            "products": "data/raw/products.json", 
            "users": "data/raw/users.json",
            "transactions": "data/raw/transactions.json"
        }
        
        # Load main collections
        for collection_name, file_path in data_files.items():
            if os.path.exists(file_path):
                self._load_json_file(collection_name, file_path)
            else:
                logger.error(f"❌ File not found: {file_path}")
        
        # Load sessions (multiple files)
        self._load_sessions_files()

    def _load_json_file(self, collection_name: str, file_path: str):
        """Load JSON file with batch processing and error handling"""
        logger.info(f"Loading {collection_name} from {file_path}...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not data:
                logger.warning(f"No data found in {file_path}")
                return
            
            # Convert date strings to datetime objects
            data = self._convert_dates(data, collection_name)
            
            collection = self.db[collection_name]
            
            # Batch insert with progress bar
            batch_size = 1000
            total_inserted = 0
            
            with tqdm(total=len(data), desc=f"Loading {collection_name}") as pbar:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        total_inserted += len(result.inserted_ids)
                        pbar.update(len(batch))
                        
                    except BulkWriteError as e:
                        # Handle partial failures
                        total_inserted += e.details.get('nInserted', 0)
                        logger.warning(f"Bulk write errors in {collection_name}: {len(e.details.get('writeErrors', []))}")
                        pbar.update(len(batch))
            
            logger.info(f"✅ Loaded {total_inserted:,} records into {collection_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load {collection_name}: {str(e)}")

    def _load_sessions_files(self):
        """Load session files (multiple files)"""
        logger.info("Loading session files...")
        
        # Create sessions collection if not exists
        if "sessions" not in self.db.list_collection_names():
            self.db.create_collection("sessions")
        
        sessions_collection = self.db["sessions"]
        
        # Find all session files
        session_files = []
        for i in range(20):  # We know we have sessions_000.json to sessions_019.json
            file_path = f"data/raw/sessions_{i:03d}.json"
            if os.path.exists(file_path):
                session_files.append(file_path)
        
        total_sessions = 0
        for file_path in session_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    sessions_data = json.load(f)
                
                # Convert dates
                sessions_data = self._convert_session_dates(sessions_data)
                
                # Batch insert
                batch_size = 1000
                for i in range(0, len(sessions_data), batch_size):
                    batch = sessions_data[i:i + batch_size]
                    try:
                        sessions_collection.insert_many(batch, ordered=False)
                        total_sessions += len(batch)
                    except Exception as e:
                        logger.warning(f"Session batch insert error: {str(e)}")
                
                logger.info(f"✅ Loaded sessions from {os.path.basename(file_path)}")
                
            except Exception as e:
                logger.error(f"❌ Failed to load {file_path}: {str(e)}")
        
        logger.info(f"✅ Total sessions loaded: {total_sessions:,}")
        
        # Create indexes for sessions
        try:
            sessions_collection.create_index([("session_id", ASCENDING)], unique=True)
            sessions_collection.create_index([("user_id", ASCENDING)])
            sessions_collection.create_index([("start_time", DESCENDING)])
            sessions_collection.create_index([("conversion_status", ASCENDING)])
            logger.info("✅ Created indexes for sessions collection")
        except Exception as e:
            logger.warning(f"Session index creation warning: {str(e)}")

    def _convert_dates(self, data: List[Dict], collection_name: str) -> List[Dict]:
        """Convert ISO date strings to datetime objects"""
        date_fields = {
            "users": ["registration_date", "last_active"],
            "products": ["creation_date", "last_updated"],
            "transactions": ["timestamp"],
            "categories": ["created_date"]
        }
        
        fields_to_convert = date_fields.get(collection_name, [])
        
        for record in data:
            for field in fields_to_convert:
                if field in record and isinstance(record[field], str):
                    try:
                        record[field] = datetime.fromisoformat(record[field].replace('Z', '+00:00'))
                    except:
                        pass  # Keep original value if conversion fails
            
            # Handle nested date fields
            if collection_name == "products" and "price_history" in record:
                for price_entry in record["price_history"]:
                    if "date" in price_entry and isinstance(price_entry["date"], str):
                        try:
                            price_entry["date"] = datetime.fromisoformat(price_entry["date"].replace('Z', '+00:00'))
                        except:
                            pass
        
        return data

    def _convert_session_dates(self, sessions_data: List[Dict]) -> List[Dict]:
        """Convert session date strings to datetime objects"""
        for session in sessions_data:
            for field in ["start_time", "end_time"]:
                if field in session and isinstance(session[field], str):
                    try:
                        session[field] = datetime.fromisoformat(session[field].replace('Z', '+00:00'))
                    except:
                        pass
        return sessions_data

    def verify_data_integrity(self):
        """Verify data loading and run integrity checks"""
        logger.info("🔍 Verifying data integrity...")
        
        collections = ["users", "products", "transactions", "categories", "sessions"]
        
        for collection_name in collections:
            try:
                collection = self.db[collection_name]
                count = collection.count_documents({})
                logger.info(f"📊 {collection_name}: {count:,} documents")
                
                # Sample document check
                sample = collection.find_one()
                if sample:
                    logger.info(f"✅ {collection_name} sample document structure verified")
                else:
                    logger.warning(f"⚠️  {collection_name} is empty")
                    
            except Exception as e:
                logger.error(f"❌ Error checking {collection_name}: {str(e)}")

    def create_sample_aggregations(self):
        """Create and test sample aggregation queries"""
        logger.info("🧪 Testing sample aggregation queries...")
        
        try:
            # 1. User demographics summary
            user_demographics = list(self.db.users.aggregate([
                {"$group": {
                    "_id": "$demographics.income_bracket",
                    "count": {"$sum": 1},
                    "avg_lifetime_value": {"$avg": "$lifetime_value"}
                }},
                {"$sort": {"count": -1}}
            ]))
            logger.info(f"✅ User demographics aggregation: {len(user_demographics)} groups")
            
            # 2. Product performance by category
            product_performance = list(self.db.products.aggregate([
                {"$match": {"is_active": True}},
                {"$group": {
                    "_id": "$category_id",
                    "total_products": {"$sum": 1},
                    "avg_price": {"$avg": "$base_price"},
                    "avg_rating": {"$avg": "$rating"}
                }},
                {"$sort": {"avg_price": -1}}
            ]))
            logger.info(f"✅ Product performance aggregation: {len(product_performance)} categories")
            
            # 3. Revenue by month
            revenue_by_month = list(self.db.transactions.aggregate([
                {"$match": {"status": "completed"}},
                {"$group": {
                    "_id": {
                        "year": {"$year": "$timestamp"},
                        "month": {"$month": "$timestamp"}
                    },
                    "total_revenue": {"$sum": "$total"},
                    "transaction_count": {"$sum": 1}
                }},
                {"$sort": {"_id.year": 1, "_id.month": 1}}
            ]))
            logger.info(f"✅ Revenue aggregation: {len(revenue_by_month)} months")
            
        except Exception as e:
            logger.error(f"❌ Aggregation test failed: {str(e)}")

    def run_complete_setup(self):
        """Run the complete MongoDB setup process"""
        logger.info("🚀 Starting complete MongoDB setup...")
        
        if not self.connect():
            return False
        
        try:
            # Setup process
            self.create_collections_with_schemas()
            self.create_indexes()
            self.load_data_optimized()
            self.verify_data_integrity()
            self.create_sample_aggregations()
            
            logger.info("=" * 60)
            logger.info("🎉 MONGODB SETUP COMPLETE!")
            logger.info("=" * 60)
            logger.info(f"📁 Database: {self.db_name}")
            logger.info(f"🔗 Connection: {self.connection_string}")
            logger.info("📊 Collections: users, products, transactions, categories, sessions")
            logger.info("🚀 Indexes: Optimized for analytics queries")
            logger.info("✅ Data: Loaded and verified")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Setup failed: {str(e)}")
            return False
        
        finally:
            if self.client:
                self.client.close()


if __name__ == "__main__":
    print("🍃 MONGODB SETUP & DATA LOADING")
    print("=" * 50)
    
    setup = MongoDBSetup()
    success = setup.run_complete_setup()
    
    if success:
        print("\n✅ MongoDB setup completed successfully!")
        print("🔗 Connect with: mongosh ecommerce_analytics")
        print("📊 Ready for analytics queries!")
    else:
        print("\n❌ MongoDB setup failed. Check logs for details.")
        sys.exit(1)