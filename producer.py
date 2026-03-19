import time
import uuid
import json
import random
from datetime import datetime
from kafka import KafkaProducer

class SimulationConfig:
    TOPIC = 'retail-events'
    BOOTSTRAP_SERVERS = ['localhost:9092']
    BASE_DELAY = 0.2
    PEAK_HOURS = [18, 19, 20, 21, 22]
    SLEEP_HOURS = [2, 3, 4, 5]        
    
    DEVICE_TYPES = ['Mobile', 'Desktop', 'Tablet']
    DEVICE_WEIGHTS = [70, 25, 5]

    EVENT_TYPES = ['view', 'addtocart', 'transaction']
    EVENT_WEIGHTS = [70, 22, 8]

    PAYMENT_STATUSES = ['Success', 'Failed']
    PAYMENT_WEIGHTS = [90, 10]

    PROMO_OPTIONS = [True, False]
    PROMO_WEIGHTS = [15, 85]

class EcommerceEventProducer:
    def __init__(self):
        self.producer = None
        self.metrics = {
            'total_events': 0,
            'transactions': 0,
            'failed_payments': 0,
            'start_time': time.time()
        }

        self.user_agents = [
            {'type': 'Mobile', 'os': 'Android', 'browser': 'Chrome Mobile', 'weight': 50},
            {'type': 'Mobile', 'os': 'iOS', 'browser': 'Safari Mobile', 'weight': 20},
            {'type': 'Desktop', 'os': 'Windows', 'browser': 'Chrome', 'weight': 15},
            {'type': 'Desktop', 'os': 'MacOS', 'browser': 'Safari', 'weight': 10},
            {'type': 'Tablet', 'os': 'Android', 'browser': 'Samsung Internet', 'weight': 5}
        ]
        self.ua_weights = [ua['weight'] for ua in self.user_agents]
        
        self.locations = [
            {'city': 'Casablanca', 'lat': 33.5731, 'lon': -7.5898, 'region': 'Casa-Settat', 'weight': 100},
            {'city': 'Mohammedia', 'lat': 33.6873, 'lon': -7.3829, 'region': 'Casa-Settat', 'weight': 30},
            {'city': 'El Jadida', 'lat': 33.2316, 'lon': -8.5007, 'region': 'Casa-Settat', 'weight': 25},
            {'city': 'Settat', 'lat': 33.0010, 'lon': -7.6166, 'region': 'Casa-Settat', 'weight': 20},
            {'city': 'Berrechid', 'lat': 33.2655, 'lon': -7.5875, 'region': 'Casa-Settat', 'weight': 15},
            {'city': 'Rabat', 'lat': 34.0209, 'lon': -6.8416, 'region': 'Rabat-Salé-Kénitra', 'weight': 80},
            {'city': 'Salé', 'lat': 34.0371, 'lon': -6.8278, 'region': 'Rabat-Salé-Kénitra', 'weight': 60},
            {'city': 'Kenitra', 'lat': 34.2610, 'lon': -6.5802, 'region': 'Rabat-Salé-Kénitra', 'weight': 50},
            {'city': 'Temara', 'lat': 33.9167, 'lon': -6.9167, 'region': 'Rabat-Salé-Kénitra', 'weight': 40},
            {'city': 'Khemisset', 'lat': 33.8248, 'lon': -6.0667, 'region': 'Rabat-Salé-Kénitra', 'weight': 10},
            {'city': 'Tangier', 'lat': 35.7595, 'lon': -5.8340, 'region': 'Tanger-Tétouan', 'weight': 70},
            {'city': 'Tetouan', 'lat': 35.5785, 'lon': -5.3684, 'region': 'Tanger-Tétouan', 'weight': 40},
            {'city': 'Al Hoceima', 'lat': 35.2446, 'lon': -3.9321, 'region': 'Tanger-Tétouan', 'weight': 20},
            {'city': 'Larache', 'lat': 35.1932, 'lon': -6.1557, 'region': 'Tanger-Tétouan', 'weight': 15},
            {'city': 'Chefchaouen', 'lat': 35.1688, 'lon': -5.2636, 'region': 'Tanger-Tétouan', 'weight': 15},
            {'city': 'Fes', 'lat': 34.0181, 'lon': -5.0078, 'region': 'Fès-Meknès', 'weight': 70},
            {'city': 'Meknes', 'lat': 33.8935, 'lon': -5.5547, 'region': 'Fès-Meknès', 'weight': 60},
            {'city': 'Taza', 'lat': 34.2182, 'lon': -4.0103, 'region': 'Fès-Meknès', 'weight': 15},
            {'city': 'Ifrane', 'lat': 33.5273, 'lon': -5.1074, 'region': 'Fès-Meknès', 'weight': 10},
            {'city': 'Marrakech', 'lat': 31.6295, 'lon': -7.9811, 'region': 'Marrakech-Safi', 'weight': 90},
            {'city': 'Safi', 'lat': 32.3008, 'lon': -9.2272, 'region': 'Marrakech-Safi', 'weight': 30},
            {'city': 'Essaouira', 'lat': 31.5085, 'lon': -9.7595, 'region': 'Marrakech-Safi', 'weight': 20},
            {'city': 'Agadir', 'lat': 30.4278, 'lon': -9.5981, 'region': 'Souss-Massa', 'weight': 60},
            {'city': 'Taroudant', 'lat': 30.4703, 'lon': -8.8770, 'region': 'Souss-Massa', 'weight': 20},
            {'city': 'Tiznit', 'lat': 29.6974, 'lon': -9.7316, 'region': 'Souss-Massa', 'weight': 15},
            {'city': 'Oujda', 'lat': 34.6814, 'lon': -1.9086, 'region': 'Oriental', 'weight': 40},
            {'city': 'Nador', 'lat': 35.1667, 'lon': -2.9333, 'region': 'Oriental', 'weight': 35},
            {'city': 'Berkane', 'lat': 34.9200, 'lon': -2.3200, 'region': 'Oriental', 'weight': 20},
            {'city': 'Beni Mellal', 'lat': 32.3394, 'lon': -6.3608, 'region': 'Béni Mellal', 'weight': 30},
            {'city': 'Khouribga', 'lat': 32.8811, 'lon': -6.9063, 'region': 'Béni Mellal', 'weight': 25},
            {'city': 'Khenifra', 'lat': 32.9395, 'lon': -5.6676, 'region': 'Béni Mellal', 'weight': 15},
            {'city': 'Laayoune', 'lat': 27.1253, 'lon': -13.1625, 'region': 'Laâyoune', 'weight': 25},
            {'city': 'Dakhla', 'lat': 23.7000, 'lon': -15.9500, 'region': 'Dakhla', 'weight': 20},
            {'city': 'Guelmim', 'lat': 28.9870, 'lon': -10.0574, 'region': 'Guelmim-Oued Noun', 'weight': 15},
            {'city': 'Tan-Tan', 'lat': 28.4380, 'lon': -11.1032, 'region': 'Guelmim-Oued Noun', 'weight': 10},
            {'city': 'Errachidia', 'lat': 31.9322, 'lon': -4.4233, 'region': 'Drâa-Tafilalet', 'weight': 15},
            {'city': 'Ouarzazate', 'lat': 30.9202, 'lon': -6.9109, 'region': 'Drâa-Tafilalet', 'weight': 20},
            {'city': 'Zagora', 'lat': 30.3324, 'lon': -5.8384, 'region': 'Drâa-Tafilalet', 'weight': 10}
        ]
        self.city_weights = [loc['weight'] for loc in self.locations]

        self.products = [
            {'id': 'PHN-001', 'cat': 'Electronics', 'sub': 'Smartphone', 'brand': 'Apple', 'name': 'iPhone 15 Pro Max', 'price': 15000},
            {'id': 'PHN-002', 'cat': 'Electronics', 'sub': 'Smartphone', 'brand': 'Samsung', 'name': 'Galaxy S24 Ultra', 'price': 12500},
            {'id': 'PHN-003', 'cat': 'Electronics', 'sub': 'Smartphone', 'brand': 'Samsung', 'name': 'Galaxy A54', 'price': 3500},
            {'id': 'PHN-004', 'cat': 'Electronics', 'sub': 'Smartphone', 'brand': 'Xiaomi', 'name': 'Redmi Note 13', 'price': 2200},
            {'id': 'PHN-005', 'cat': 'Electronics', 'sub': 'Smartphone', 'brand': 'Infinix', 'name': 'Hot 40 Pro', 'price': 1800},
            {'id': 'LPT-001', 'cat': 'Electronics', 'sub': 'Laptop', 'brand': 'Apple', 'name': 'MacBook Air M2', 'price': 11500},
            {'id': 'LPT-002', 'cat': 'Electronics', 'sub': 'Laptop', 'brand': 'HP', 'name': 'Victus Gaming', 'price': 8500},
            {'id': 'LPT-003', 'cat': 'Electronics', 'sub': 'Laptop', 'brand': 'Lenovo', 'name': 'IdeaPad 3', 'price': 5000},
            {'id': 'ACC-001', 'cat': 'Electronics', 'sub': 'Accessories', 'brand': 'Apple', 'name': 'AirPods Pro 2', 'price': 2800},
            {'id': 'ACC-002', 'cat': 'Electronics', 'sub': 'Accessories', 'brand': 'JBL', 'name': 'Bluetooth Speaker Flip 6', 'price': 1100},
            {'id': 'GMG-001', 'cat': 'Gaming', 'sub': 'Console', 'brand': 'Sony', 'name': 'PlayStation 5 Slim', 'price': 6200},
            {'id': 'GMG-002', 'cat': 'Gaming', 'sub': 'Console', 'brand': 'Nintendo', 'name': 'Switch OLED', 'price': 3900},
            {'id': 'GMG-003', 'cat': 'Gaming', 'sub': 'Game', 'brand': 'EA Sports', 'name': 'FC 24 (FIFA)', 'price': 600},
            {'id': 'GMG-004', 'cat': 'Gaming', 'sub': 'Accessories', 'brand': 'Logitech', 'name': 'Mouse G502 Hero', 'price': 550},
            {'id': 'KIT-001', 'cat': 'Home', 'sub': 'Kitchen', 'brand': 'Ninja', 'name': 'Air Fryer Dual Zone', 'price': 2200},
            {'id': 'KIT-002', 'cat': 'Home', 'sub': 'Kitchen', 'brand': 'Nespresso', 'name': 'Inissia Machine', 'price': 1200},
            {'id': 'KIT-003', 'cat': 'Home', 'sub': 'Kitchen', 'brand': 'Moulinex', 'name': 'Blender Uno', 'price': 450},
            {'id': 'HOM-001', 'cat': 'Home', 'sub': 'Cleaning', 'brand': 'Dyson', 'name': 'V15 Detect', 'price': 6500},
            {'id': 'HOM-002', 'cat': 'Home', 'sub': 'Cleaning', 'brand': 'Roborock', 'name': 'Robot Aspirateur S8', 'price': 4800},
            {'id': 'HOM-003', 'cat': 'Home', 'sub': 'Furniture', 'brand': 'IKEA', 'name': 'Gaming Chair Markus', 'price': 1800},
            {'id': 'FSH-001', 'cat': 'Fashion', 'sub': 'Shoes', 'brand': 'Nike', 'name': 'Air Force 1 White', 'price': 1200},
            {'id': 'FSH-002', 'cat': 'Fashion', 'sub': 'Shoes', 'brand': 'Adidas', 'name': 'Yeezy Boost 350', 'price': 2800},
            {'id': 'FSH-003', 'cat': 'Fashion', 'sub': 'Shoes', 'brand': 'New Balance', 'name': '530 Retro', 'price': 1100},
            {'id': 'CLT-001', 'cat': 'Fashion', 'sub': 'Clothing', 'brand': 'Zara', 'name': 'Blazer Homme', 'price': 899},
            {'id': 'CLT-002', 'cat': 'Fashion', 'sub': 'Clothing', 'brand': 'H&M', 'name': 'Robe Été Fleurie', 'price': 399},
            {'id': 'CLT-003', 'cat': 'Fashion', 'sub': 'Clothing', 'brand': 'Pull&Bear', 'name': 'Jean Wide Leg', 'price': 459},
            {'id': 'ACC-003', 'cat': 'Fashion', 'sub': 'Accessories', 'brand': 'Casio', 'name': 'Montre Vintage', 'price': 600},
            {'id': 'BTY-001', 'cat': 'Beauty', 'sub': 'Perfume', 'brand': 'Dior', 'name': 'Sauvage Elixir', 'price': 1400},
            {'id': 'BTY-002', 'cat': 'Beauty', 'sub': 'Perfume', 'brand': 'YSL', 'name': 'Libre Eau de Parfum', 'price': 1100},
            {'id': 'BTY-003', 'cat': 'Beauty', 'sub': 'Skincare', 'brand': 'The Ordinary', 'name': 'Niacinamide 10%', 'price': 120},
            {'id': 'BTY-004', 'cat': 'Beauty', 'sub': 'Skincare', 'brand': 'La Roche-Posay', 'name': 'Anthelios Sunscreen', 'price': 180},
            {'id': 'BTY-005', 'cat': 'Beauty', 'sub': 'Makeup', 'brand': 'Maybelline', 'name': 'Mascara Sky High', 'price': 110},
            {'id': 'SPT-001', 'cat': 'Sports', 'sub': 'Fitness', 'brand': 'Decathlon', 'name': 'Haltères 10kg', 'price': 300},
            {'id': 'SPT-002', 'cat': 'Sports', 'sub': 'Fitness', 'brand': 'Technogym', 'name': 'Tapis de Course', 'price': 6000},
            {'id': 'SPT-003', 'cat': 'Sports', 'sub': 'Football', 'brand': 'Adidas', 'name': 'Ballon Coupe du Monde', 'price': 350}
        ]

        self.sources = [
            {'source': 'Google', 'medium': 'organic', 'campaign': 'SEO_General', 'weight': 30},
            {'source': 'Google', 'medium': 'cpc', 'campaign': 'Search_Brand_Name', 'weight': 15},
            {'source': 'Google', 'medium': 'cpc', 'campaign': 'Shopping_Electronics_Q1', 'weight': 10},
            {'source': 'Facebook', 'medium': 'cpc', 'campaign': 'Retargeting_Visitors', 'weight': 12},
            {'source': 'Instagram', 'medium': 'cpc', 'campaign': 'Story_Ads_Lifestyle', 'weight': 12},
            {'source': 'TikTok', 'medium': 'cpc', 'campaign': 'Spark_Ads_GenZ', 'weight': 10},
            {'source': 'Instagram', 'medium': 'referral', 'campaign': 'Influencer_Asmaa_Beauty', 'weight': 5},
            {'source': 'YouTube', 'medium': 'referral', 'campaign': 'Review_Tech_Maroc', 'weight': 5},
            {'source': 'TikTok', 'medium': 'referral', 'campaign': 'Challenge_Danse_Promo', 'weight': 5},
            {'source': 'Direct', 'medium': 'none', 'campaign': 'App_Launch', 'weight': 25},
            {'source': 'Email', 'medium': 'newsletter', 'campaign': 'Weekly_Best_Sellers', 'weight': 8},
            {'source': 'Email', 'medium': 'automation', 'campaign': 'Abandoned_Cart_Recovery', 'weight': 6}, 
            {'source': 'Avito', 'medium': 'referral', 'campaign': 'Display_Banner', 'weight': 3},
            {'source': 'Hespress', 'medium': 'referral', 'campaign': 'Article_Sponsorisé', 'weight': 2}
        ]
        self.sources_weights = [m['weight'] for m in self.sources]

    def connect(self):
        print("Connecting to Kafka")
        while self.producer is None:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=SimulationConfig.BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("Connected!")
            except Exception as e:
                print(f"Retrying... {e}")
                time.sleep(5)

    def get_time_multiplier(self):
        now = datetime.now()
        hour = now.hour
        is_weekend = now.weekday() >= 5
        multiplier = 1.0
        if is_weekend:
            multiplier *= 0.7  
        if hour in SimulationConfig.PEAK_HOURS:
            multiplier *= 0.5  
        elif hour in SimulationConfig.SLEEP_HOURS:
            multiplier *= 5.0  
        return multiplier

    def update_metrics(self, data):
        self.metrics['total_events'] += 1
        if data['event_type'] == 'transaction':
            if data['payment_status'] == 'Success':
                self.metrics['transactions'] += 1
            else:
                self.metrics['failed_payments'] += 1
        
        if self.metrics['total_events'] % 50 == 0:
            elapsed = time.time() - self.metrics['start_time']
            rate = self.metrics['total_events'] / elapsed
            print(f"\n--- METRICS: {rate:.2f} events/sec | Total Txn: {self.metrics['transactions']} ---\n")

    def generate_event(self):
        location = random.choices(self.locations, weights=self.city_weights, k=1)[0]
        product = random.choice(self.products)
        source_data = random.choices(self.sources, weights=self.sources_weights, k=1)[0]
        user_agent = random.choices(self.user_agents, weights=self.ua_weights, k=1)[0]

        event_type = random.choices(
            SimulationConfig.EVENT_TYPES, 
            weights=SimulationConfig.EVENT_WEIGHTS, 
            k=1
        )[0]

        price = product['price']
        is_promo = random.choices(
            SimulationConfig.PROMO_OPTIONS, 
            weights=SimulationConfig.PROMO_WEIGHTS, 
            k=1
        )[0]
        
        if is_promo: 
            price = round(price * 0.85, 2)

        status = None
        if event_type == 'transaction':
            status = random.choices(
                SimulationConfig.PAYMENT_STATUSES, 
                weights=SimulationConfig.PAYMENT_WEIGHTS, 
                k=1
            )[0]

        return {
            'event_id': str(uuid.uuid4()),
            'session_id': str(uuid.uuid4()),
            'event_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'visitor_id': random.randint(100000, 999999),
            'city': location['city'],
            'region': location['region'],
            'lat': location['lat'],
            'lon': location['lon'],
            'device_type': user_agent['type'], 
            'os': user_agent['os'],            
            'browser': user_agent['browser'],  
            'product_id': product['id'],
            'product_name': product['name'],
            'category': product['cat'],
            'subcategory': product['sub'],
            'brand': product['brand'],
            'price_unit': price,
            'event_type': event_type,
            'quantity': 1 if event_type != 'transaction' else random.randint(1, 2),
            'total_amount': 0 if event_type != 'transaction' else round(price * random.randint(1, 2), 2),
            'payment_status': status,
            'traffic_source': source_data['source'],
            'traffic_medium': source_data['medium'],
            'campaign': source_data['campaign'],
            'is_promo': is_promo
        }

    def run(self):
        if self.producer is None: self.connect()
        try:
            while True:
                data = self.generate_event()
                self.producer.send(SimulationConfig.TOPIC, data)
                self.update_metrics(data)
                
                if data['event_type'] == 'transaction':
                    if data['payment_status'] == 'Success':
                        print(f"VENTE: {data['city']} | {data['product_name']} | {data['total_amount']} DH")
                    else:
                        print(f"ECHEC: {data['city']} | Paiement refusé")
                elif data['event_type'] == 'addtocart':
                    print(f"PANIER: {data['product_name']} ({data['brand']})")
                
                delay = SimulationConfig.BASE_DELAY * self.get_time_multiplier()
                time.sleep(random.uniform(delay * 0.5, delay * 1.5))

        except KeyboardInterrupt:
            if self.producer: self.producer.close()

if __name__ == "__main__":
    EcommerceEventProducer().run()