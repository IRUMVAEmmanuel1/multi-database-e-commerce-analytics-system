# scripts/generate_dataset.py
"""
Enhanced E-commerce Dataset Generator
AUCA Big Data Analytics Final Project
Generates realistic e-commerce data for MongoDB, HBase, and Spark analytics
"""

import json
import random
import datetime
import uuid
import threading
import numpy as np
from faker import Faker
import os
from typing import Dict, List, Any
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/dataset_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EcommerceDataGenerator:
    """Professional e-commerce dataset generator with realistic business patterns"""
    
    def __init__(self):
        self.fake = Faker()
        
        # Configuration for realistic scale
        self.config = {
            'NUM_USERS': 10000,
            'NUM_PRODUCTS': 5000, 
            'NUM_CATEGORIES': 25,
            'NUM_TRANSACTIONS': 50000,
            'NUM_SESSIONS': 200000,
            'TIMESPAN_DAYS': 90,
            'CHUNK_SIZE': 10000
        }
        
        # Set seeds for reproducibility
        np.random.seed(42)
        random.seed(42)
        Faker.seed(42)
        
        # Initialize data containers
        self.categories = []
        self.products = []
        self.users = []
        self.sessions = []
        self.transactions = []
        
        # Ensure output directory exists
        os.makedirs("data/raw", exist_ok=True)
        
        logger.info("EcommerceDataGenerator initialized with professional settings")

    def generate_categories(self) -> List[Dict[str, Any]]:
        """Generate realistic product categories"""
        logger.info("Generating categories with business hierarchy...")
        
        # Realistic e-commerce categories
        category_data = [
            ("Electronics", ["Smartphones", "Laptops", "Headphones", "Cameras", "Gaming"]),
            ("Clothing", ["Men's Wear", "Women's Wear", "Kids", "Shoes", "Accessories"]),
            ("Home & Garden", ["Furniture", "Kitchen", "Decor", "Tools", "Lighting"]),
            ("Sports", ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Winter Sports"]),
            ("Books", ["Fiction", "Non-Fiction", "Educational", "Children's", "Comics"]),
            ("Health & Beauty", ["Skincare", "Makeup", "Supplements", "Personal Care", "Fragrances"]),
            ("Automotive", ["Parts", "Accessories", "Tools", "Care Products", "Electronics"]),
            ("Toys & Games", ["Educational", "Action Figures", "Board Games", "Puzzles", "Outdoor"]),
            ("Jewelry", ["Rings", "Necklaces", "Earrings", "Watches", "Bracelets"]),
            ("Food & Beverages", ["Snacks", "Beverages", "Organic", "International", "Gourmet"])
        ]
        
        for cat_id, (main_cat, subcats) in enumerate(category_data[:self.config['NUM_CATEGORIES']]):
            category = {
                "category_id": f"cat_{cat_id:03d}",
                "name": main_cat,
                "description": f"Premium {main_cat.lower()} products for modern lifestyle",
                "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
                "created_date": self.fake.date_time_between(start_date='-2y', end_date='-1y').isoformat(),
                "subcategories": []
            }
            
            for sub_id, subcat_name in enumerate(subcats):
                subcategory = {
                    "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
                    "name": subcat_name,
                    "profit_margin": round(random.uniform(0.15, 0.45), 3),
                    "commission_rate": round(random.uniform(0.03, 0.12), 3),
                    "is_featured": random.choice([True, False])
                }
                category["subcategories"].append(subcategory)
            
            self.categories.append(category)
        
        logger.info(f"Generated {len(self.categories)} categories with {sum(len(c['subcategories']) for c in self.categories)} subcategories")
        return self.categories

    def generate_products(self) -> List[Dict[str, Any]]:
        """Generate realistic products with market-based pricing"""
        logger.info("Generating products with realistic market data...")
        
        # Price ranges by category
        price_ranges = {
            "Electronics": (50, 2000),
            "Clothing": (15, 300),
            "Home & Garden": (25, 800),
            "Sports": (20, 500),
            "Books": (5, 50),
            "Health & Beauty": (10, 150),
            "Automotive": (15, 400),
            "Toys & Games": (8, 200),
            "Jewelry": (30, 1000),
            "Food & Beverages": (3, 80)
        }
        
        brands_by_category = {
            "Electronics": ["TechCorp", "InnovateTech", "DigitalPro", "SmartDevices", "FutureTech"],
            "Clothing": ["StyleCo", "FashionForward", "UrbanWear", "ClassicStyle", "ModernFit"],
            "Home & Garden": ["HomeComfort", "LivingStyle", "GardenPro", "DecorPlus", "CozyHome"]
        }
        
        product_creation_start = datetime.datetime.now() - datetime.timedelta(days=self.config['TIMESPAN_DAYS']*2)
        
        for prod_id in tqdm(range(self.config['NUM_PRODUCTS']), desc="Creating products"):
            category = random.choice(self.categories)
            subcategory = random.choice(category["subcategories"])
            
            # Price based on category
            cat_name = category["name"]
            min_price, max_price = price_ranges.get(cat_name, (10, 100))
            base_price = round(random.uniform(min_price, max_price), 2)
            
            # Generate price history (market fluctuations)
            price_history = self._generate_price_history(base_price, product_creation_start)
            current_price = price_history[-1]["price"]
            
            # Brand selection
            available_brands = brands_by_category.get(cat_name, ["GenericBrand", "QualityMaker", "ReliableCorp"])
            brand = random.choice(available_brands)
            
            product = {
                "product_id": f"prod_{prod_id:05d}",
                "sku": f"{brand[:3].upper()}-{random.randint(100000, 999999)}",
                "name": self._generate_product_name(category["name"], subcategory["name"]),
                "description": self.fake.text(max_nb_chars=250),
                "category_id": category["category_id"],
                "subcategory_id": subcategory["subcategory_id"],
                "brand": brand,
                "base_price": current_price,
                "cost": round(current_price * (1 - subcategory["profit_margin"]), 2),
                "current_stock": random.randint(0, 1000),
                "reorder_level": random.randint(10, 50),
                "weight_kg": round(random.uniform(0.1, 25.0), 2),
                "dimensions": {
                    "length_cm": round(random.uniform(5, 120), 1),
                    "width_cm": round(random.uniform(5, 80), 1),
                    "height_cm": round(random.uniform(2, 40), 1)
                },
                "is_active": random.choices([True, False], weights=[0.92, 0.08])[0],
                "rating": round(random.uniform(1.0, 5.0), 1),
                "review_count": random.randint(0, 500),
                "price_history": price_history,
                "creation_date": price_history[0]["date"],
                "last_updated": price_history[-1]["date"],
                "tags": self._generate_product_tags(),
                "seasonal": random.choice([True, False]),
                "featured": random.choices([True, False], weights=[0.1, 0.9])[0]
            }
            
            self.products.append(product)
        
        logger.info(f"Generated {len(self.products)} products with realistic market data")
        return self.products

    def _generate_price_history(self, base_price: float, start_date: datetime.datetime) -> List[Dict]:
        """Generate realistic price fluctuation history"""
        price_history = []
        current_price = base_price
        current_date = start_date
        
        # Initial price
        price_history.append({
            "price": base_price,
            "date": current_date.isoformat(),
            "reason": "initial_listing"
        })
        
        # Generate 0-4 price changes (market dynamics)
        num_changes = random.randint(0, 4)
        for _ in range(num_changes):
            # Time between price changes
            days_forward = random.randint(7, 45)
            current_date += datetime.timedelta(days=days_forward)
            
            # Price change logic (realistic market behavior)
            change_type = random.choices(
                ["promotion", "market_adjustment", "cost_increase", "clearance", "restock"],
                weights=[0.3, 0.25, 0.2, 0.15, 0.1]
            )[0]
            
            if change_type == "promotion":
                change_factor = random.uniform(0.7, 0.9)  # 10-30% discount
            elif change_type == "clearance":
                change_factor = random.uniform(0.5, 0.8)  # 20-50% discount
            elif change_type == "cost_increase":
                change_factor = random.uniform(1.05, 1.25)  # 5-25% increase
            else:
                change_factor = random.uniform(0.9, 1.1)  # Minor adjustment
            
            current_price = round(current_price * change_factor, 2)
            
            price_history.append({
                "price": current_price,
                "date": current_date.isoformat(),
                "reason": change_type
            })
        
        return price_history

    def _generate_product_name(self, category: str, subcategory: str) -> str:
        """Generate realistic product names"""
        adjectives = ["Premium", "Professional", "Advanced", "Deluxe", "Essential", "Classic", "Modern", "Ultimate"]
        descriptors = ["Pro", "Plus", "Elite", "Standard", "Compact", "Extended", "Smart", "Enhanced"]
        
        adjective = random.choice(adjectives)
        descriptor = random.choice(descriptors)
        
        return f"{adjective} {subcategory} {descriptor}"

    def _generate_product_tags(self) -> List[str]:
        """Generate relevant product tags"""
        all_tags = [
            "bestseller", "new_arrival", "eco_friendly", "premium", "budget", 
            "limited_edition", "trending", "featured", "sale", "recommended",
            "organic", "handmade", "imported", "local", "certified", "award_winning"
        ]
        return random.sample(all_tags, k=random.randint(0, 4))

    def generate_users(self) -> List[Dict[str, Any]]:
        """Generate diverse user profiles with realistic demographics"""
        logger.info("Generating user profiles with demographic data...")
        
        # Realistic demographic distributions
        age_groups = [(18, 25), (26, 35), (36, 45), (46, 55), (56, 70)]
        age_weights = [0.15, 0.30, 0.25, 0.20, 0.10]
        
        income_brackets = ["low", "medium", "high", "premium"]
        income_weights = [0.25, 0.40, 0.25, 0.10]
        
        for user_id in tqdm(range(self.config['NUM_USERS']), desc="Creating users"):
            # Demographics
            age_range = random.choices(age_groups, weights=age_weights)[0]
            age = random.randint(*age_range)
            
            income_bracket = random.choices(income_brackets, weights=income_weights)[0]
            
            reg_date = self.fake.date_time_between(
                start_date=f"-{self.config['TIMESPAN_DAYS']*3}d",
                end_date=f"-{self.config['TIMESPAN_DAYS']}d"
            )
            
            user = {
                "user_id": f"user_{user_id:06d}",
                "email": self.fake.email(),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "demographics": {
                    "age": age,
                    "gender": random.choice(["M", "F", "Other"]),
                    "income_bracket": income_bracket,
                    "education": random.choice(["high_school", "bachelor", "master", "phd", "other"]),
                    "occupation": self.fake.job(),
                    "marital_status": random.choice(["single", "married", "divorced", "widowed"])
                },
                "geo_data": {
                    "city": self.fake.city(),
                    "state": self.fake.state_abbr(),
                    "country": random.choices(["US", "CA", "UK", "DE", "FR"], weights=[0.6, 0.15, 0.1, 0.08, 0.07])[0],
                    "timezone": self.fake.timezone(),
                    "postal_code": self.fake.zipcode()
                },
                "preferences": {
                    "preferred_categories": random.sample(
                        [cat["category_id"] for cat in self.categories if cat["is_active"]], 
                        k=random.randint(1, 5)
                    ),
                    "communication_email": random.choices([True, False], weights=[0.8, 0.2])[0],
                    "communication_sms": random.choices([True, False], weights=[0.6, 0.4])[0],
                    "marketing_consent": random.choices([True, False], weights=[0.7, 0.3])[0],
                    "language": random.choices(["en", "es", "fr", "de"], weights=[0.7, 0.15, 0.1, 0.05])[0]
                },
                "registration_date": reg_date.isoformat(),
                "last_active": self.fake.date_time_between(start_date=reg_date, end_date="now").isoformat(),
                "account_status": random.choices(
                    ["active", "inactive", "suspended"], 
                    weights=[0.85, 0.14, 0.01]
                )[0],
                "loyalty_tier": random.choices(
                    ["bronze", "silver", "gold", "platinum"],
                    weights=[0.6, 0.25, 0.12, 0.03]
                )[0],
                "total_orders": 0,  # Will be updated during transaction generation
                "lifetime_value": 0.0  # Will be calculated
            }
            
            self.users.append(user)
        
        logger.info(f"Generated {len(self.users)} diverse user profiles")
        return self.users

    def save_data(self):
        """Save all generated data with proper organization"""
        logger.info("Saving datasets to organized structure...")
        
        def json_serializer(obj):
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        # Save individual datasets
        datasets = {
            "categories.json": self.categories,
            "products.json": self.products,
            "users.json": self.users,
            "transactions.json": self.transactions
        }
        
        for filename, data in datasets.items():
            filepath = os.path.join("data/raw", filename)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, default=json_serializer, indent=2, ensure_ascii=False)
            logger.info(f" Saved {len(data):,} records to {filepath}")
        
        # Save sessions in chunks for better memory management
        chunk_size = self.config['CHUNK_SIZE']
        for i in range(0, len(self.sessions), chunk_size):
            chunk = self.sessions[i:i+chunk_size]
            filename = f"sessions_{i//chunk_size:03d}.json"
            filepath = os.path.join("data/raw", filename)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(chunk, f, default=json_serializer, indent=2, ensure_ascii=False)
            logger.info(f" Saved {len(chunk):,} sessions to {filepath}")
        
        # Generate comprehensive summary
        self._generate_summary()

    def _generate_summary(self):
        """Generate detailed dataset statistics"""
        total_revenue = sum(t["total"] for t in self.transactions)
        avg_transaction = total_revenue / len(self.transactions) if self.transactions else 0
        conversion_rate = len(self.transactions) / len(self.sessions) if self.sessions else 0
        
        summary = {
            "generation_metadata": {
                "timestamp": datetime.datetime.now().isoformat(),
                "generator_version": "2.0",
                "configuration": self.config
            },
            "data_volumes": {
                "categories": len(self.categories),
                "products": len(self.products),
                "users": len(self.users),
                "sessions": len(self.sessions),
                "transactions": len(self.transactions),
                "total_records": len(self.categories) + len(self.products) + len(self.users) + len(self.sessions) + len(self.transactions)
            },
            "business_metrics": {
                "total_revenue": round(total_revenue, 2),
                "average_transaction_value": round(avg_transaction, 2),
                "conversion_rate": round(conversion_rate * 100, 2),
                "active_products": sum(1 for p in self.products if p["is_active"]),
                "active_users": sum(1 for u in self.users if u["account_status"] == "active"),
                "average_product_rating": round(sum(p["rating"] for p in self.products) / len(self.products), 2),
                "total_inventory_value": sum(p["base_price"] * p["current_stock"] for p in self.products)
            },
            "data_quality": {
                "products_with_reviews": sum(1 for p in self.products if p["review_count"] > 0),
                "users_with_preferences": sum(1 for u in self.users if u["preferences"]["preferred_categories"]),
                "transactions_with_discounts": sum(1 for t in self.transactions if t.get("discount", 0) > 0)
            },
            "time_range": {
                "start_date": min(s["start_time"] for s in self.sessions) if self.sessions else None,
                "end_date": max(s["end_time"] for s in self.sessions) if self.sessions else None
            }
        }
        
        # Save summary
        summary_path = "data/raw/dataset_summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        
        # Log key metrics
        logger.info("=" * 60)
        logger.info(" DATASET GENERATION COMPLETE!")
        logger.info("=" * 60)
        logger.info(f" Total Records: {summary['data_volumes']['total_records']:,}")
        logger.info(f"💰 Total Revenue: ${summary['business_metrics']['total_revenue']:,.2f}")
        logger.info(f"🛒 Avg Transaction: ${summary['business_metrics']['average_transaction_value']:.2f}")
        logger.info(f" Conversion Rate: {summary['business_metrics']['conversion_rate']:.2f}%")
        logger.info(f"⭐ Avg Rating: {summary['business_metrics']['average_product_rating']:.1f}/5.0")
        logger.info("=" * 60)

    def generate_all(self):
        """Generate complete realistic e-commerce dataset"""
        logger.info(" Starting comprehensive e-commerce dataset generation...")
        
        try:
            # Generate core data
            self.generate_categories()
            self.generate_products()
            self.generate_users()
            
            # Generate behavioral data (simplified for now)
            logger.info("Generating transactions and sessions...")
            self._generate_simple_transactions()
            self._generate_simple_sessions()
            
            # Save everything
            self.save_data()
            
            logger.info(" Dataset generation completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f" Dataset generation failed: {str(e)}")
            raise

    def _generate_simple_transactions(self):
        """Generate realistic transactions"""
        logger.info("Creating realistic transaction patterns...")
        
        # Generate transactions with realistic patterns
        for _ in tqdm(range(self.config['NUM_TRANSACTIONS']), desc="Creating transactions"):
            user = random.choice([u for u in self.users if u["account_status"] == "active"])
            
            # Select 1-5 products for transaction
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.1, 0.05])[0]
            available_products = [p for p in self.products if p["is_active"] and p["current_stock"] > 0]
            
            if len(available_products) < num_items:
                continue
                
            selected_products = random.sample(available_products, num_items)
            
            items = []
            subtotal = 0
            
            for product in selected_products:
                quantity = random.randint(1, min(3, product["current_stock"]))
                unit_price = product["base_price"]
                item_subtotal = quantity * unit_price
                
                items.append({
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "subtotal": round(item_subtotal, 2)
                })
                
                subtotal += item_subtotal
                # Update stock
                product["current_stock"] -= quantity
            
            # Apply discounts
            discount = 0
            if random.random() < 0.25:  # 25% chance of discount
                discount_rate = random.choice([0.05, 0.10, 0.15, 0.20])
                discount = round(subtotal * discount_rate, 2)
            
            # Calculate tax and shipping
            tax = round(subtotal * 0.08, 2)  # 8% tax
            shipping = 0 if subtotal > 50 else 9.99  # Free shipping over $50
            
            total = round(subtotal - discount + tax + shipping, 2)
            
            transaction = {
                "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                "user_id": user["user_id"],
                "timestamp": self.fake.date_time_between(
                    start_date=f"-{self.config['TIMESPAN_DAYS']}d",
                    end_date="now"
                ).isoformat(),
                "items": items,
                "subtotal": round(subtotal, 2),
                "discount": discount,
                "tax": tax,
                "shipping": shipping,
                "total": total,
                "payment_method": random.choice([
                    "credit_card", "debit_card", "paypal", "apple_pay", 
                    "google_pay", "bank_transfer", "gift_card"
                ]),
                "status": random.choices(
                    ["completed", "processing", "shipped", "delivered", "cancelled"],
                    weights=[0.7, 0.1, 0.1, 0.08, 0.02]
                )[0],
                "billing_address": user["geo_data"],
                "shipping_address": user["geo_data"]
            }
            
            self.transactions.append(transaction)
            
            # Update user metrics
            user["total_orders"] += 1
            user["lifetime_value"] += total

    def _generate_simple_sessions(self):
        """Generate realistic user sessions"""
        logger.info("Creating realistic user session patterns...")
        
        for _ in tqdm(range(self.config['NUM_SESSIONS']), desc="Creating sessions"):
            user = random.choice(self.users)
            
            session = {
                "session_id": f"sess_{uuid.uuid4().hex[:10]}",
                "user_id": user["user_id"],
                "start_time": self.fake.date_time_between(
                    start_date=f"-{self.config['TIMESPAN_DAYS']}d",
                    end_date="now"
                ).isoformat(),
                "duration_seconds": random.randint(30, 3600),
                "device_type": random.choice(["mobile", "desktop", "tablet"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
                "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                "geo_data": {
                    **user["geo_data"],
                    "ip_address": self.fake.ipv4()
                },
                "pages_viewed": random.randint(1, 15),
                "products_viewed": random.sample(
                    [p["product_id"] for p in self.products[:100]], 
                    k=random.randint(0, 5)
                ),
                "conversion_status": random.choices(
                    ["converted", "abandoned", "browsed"],
                    weights=[0.03, 0.15, 0.82]
                )[0],
                "referrer": random.choice([
                    "direct", "search_engine", "social_media", 
                    "email", "affiliate", "ads"
                ])
            }
            
            # Set end time
            start_time = datetime.datetime.fromisoformat(session["start_time"])
            end_time = start_time + datetime.timedelta(seconds=session["duration_seconds"])
            session["end_time"] = end_time.isoformat()
            
            self.sessions.append(session)


class InventoryManager:
    """Thread-safe inventory management"""
    
    def __init__(self, products: List[Dict]):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()
    
    def update_stock(self, product_id: str, quantity: int) -> bool:
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False
    
    def get_product(self, product_id: str) -> Dict:
        with self.lock:
            return self.products.get(product_id, {})


if __name__ == "__main__":
    print("🏪 E-COMMERCE DATASET GENERATOR")
    print("=" * 50)
    
    generator = EcommerceDataGenerator()
    success = generator.generate_all()
    
    if success:
        print("\n Dataset generation completed successfully!")
        print(" Check the data/raw/ directory for generated files")
        print(" Review data/raw/dataset_summary.json for statistics")
    else:
        print("\n Dataset generation failed. Check logs for details.")