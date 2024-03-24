import numpy as np

# np.random.seed(42)

transaction_schema = {
    "transaction_id": str,
    "product_id": str,
    "timestamp": str,
    "quantity": int,
    "unit_price": float,
    "store_id": str
}

inventory_schema = {
    "inventory_id": str,
    "product_id": str,
    "timestamp": str,
    "quantity_change": int,
    "store_id": str
}

products_schema = {
    "product_id": str,
    "name": str,
    "category": str,
    "base_price": float,
    "supplier_id": str
}

stores_schema = {
    "store_id": str,
    # "name": str,
    "location": str,
    "size": int,
    "manager": str
}

electronic_product_names = [
    "Smart TV",
    "Laptop Pro X",
    "Wireless Earbuds Elite",
    "Digital Camera 360",
    "Gaming Console X-Gen",
    "Smartwatch Galaxy Fit",
    "Bluetooth Speaker BassMaster",
    "Virtual Reality Headset Vortex",
    "High-Performance Router TurboNet",
    "Noise-Canceling Headphones Zenith"
]
electronic_price_ranges = [
    (500, 2000),
    (800, 2500),
    (100, 300),
    (300, 1200),
    (400, 600),
    (150, 400),
    (50, 200),
    (300, 800),
    (100, 300),
    (150, 350),
]

clothing_product_names = [
    "Denim Jeans Classic Fit",
    "Sneaker StreetWalker",
    "Active Wear FlexMotion Set",
    "Leather Jacket UrbanEdge",
    "Floral Print Summer Dress",
    "Running Shoes SwiftStride",
    "Knit Sweater CozyBlend",
    "Sunglasses MirageShade",
    "Formal Suit Elegance Ensemble",
    "Yoga Pants BreathEase",
]
clothing_price_ranges = [
    (40, 100),
    (60, 150),
    (50, 120),
    (100, 300),
    (30, 80),
    (80, 150),
    (50, 120),
    (20, 80),
    (150, 500),
    (40, 100),
]

appliances_product_names = [
    "Stainless Steel Blender Fusion",
    "Robot Vacuum Cleaner SmartSweep",
    "Coffee Maker BrewMaster",
    "Air Purifier PureBreeze",
    "Non-Stick Cookware Set CulinaryPro",
    "Smart Thermostat EcoTemp",
    "Robotic Lawn Mower GreenGroom",
    "Digital Weight Scale SlimFit",
    "Espresso Machine EspressoElite",
    "Slow Cooker SavoryStew",
]
appliances_price_ranges = [
    (50, 150),
    (200, 500),
    (30, 100),
    (100, 300),
    (80, 200),
    (80, 150),
    (300, 800),
    (20, 80),
    (100, 300),
    (40, 100),
]

books_product_names = [
    'Mystery Novel: "Whispers in the Shadows"',
    'Inspirational Journal: "Journey of Gratitude"',
    'Science Fiction Book: "Galactic Odyssey"',
    'Educational Textbook: "Mathematics Mastery"',
    'Adult Coloring Book: "Serenity Patterns"',
    'Fictional Fantasy Novel: "Realm of Dragons"',
    'Classic Literature: "Pride and Prejudice"',
    'Memoir: "Beneath the Surface"',
    'Historical Fiction: "The Silk Road Chronicles"',
    'Poetry Collection: "Verses of the Heart"',
]
books_price_ranges = [
    (10, 25),
    (15, 30),
    (12, 28),
    (30, 80),
    (10, 20),
    (12, 25),
    (8, 20),
    (15, 35),
    (10, 25),
    (10, 20),
]

sports_product_names = [
    "Mountain Bike TrailBlazer",
    "Camping Tent Adventure Dome",
    "Yoga Mat FlexGrip",
    "Golf Clubs ProSeries",
    "Hiking Backpack Summit Seeker",
    "Kayak Explorer XT",
    "Baseball Glove DiamondCatch",
    "Fishing Rod ExtremeCast",
    "Running Shoes TrailBlaze Runner",
    "Soccer Ball PrecisionStrike",
]
sports_price_ranges = [
    (300, 1000),
    (50, 200),
    (20, 50),
    (200, 800),
    (80, 250),
    (300, 800),
    (30, 100),
    (50, 150),
    (80, 150),
    (20, 50),
]

start_product_id = 100
start_supplier_id = 100
product_category = {
    "product_id": [list(range(100, 110)),list(range(200, 210)), list(range(300, 310)), list(range(400, 410)), list(range(500, 510))],
    "supplier_id": [list(range(100, 110)),list(range(200, 210)), list(range(300, 310)), list(range(400, 410)), list(range(500, 510))],
    "category": ["Electronics", "Clothing and Apparel", "Home and Kitchen Appliances", "Books and Stationery", "Sports and Outdoor Gear"],
    "name": [electronic_product_names, clothing_product_names, appliances_product_names, books_product_names, sports_product_names],
    "base_price": [electronic_price_ranges, clothing_price_ranges, appliances_price_ranges, books_price_ranges, sports_price_ranges]
}

start_store_id = 100
stores = {
    "store_id":list(range(100, 600, 50)),
    # "name": ["Tech Haven", "Fashion Fusion", "Home Harmony", "Book Bliss", "Adventure Outpost",
    #                        "Gadget Galaxy", "Chic Boutique", "Kitchen Kingdom", "Page Turner Emporium", "Outdoor Oasis"],
    "location": ["Huntsville, AL", "Anchorage, AK", "Phoenix, AZ", "Little Rock, AR", "New York, NY", "Los Angeles, CA", 
                 "Chicago, IL", "Wilmington, DE", "Washington, DC", "Honolulu, HI", "Boise, ID", "Houston, TX", "Miami, FL",
                "Omaha, NE", "Baltimore, MD", "Providence, RI", "Philadelphia, PA", "Seattle, WA", "Atlanta, GA", "Denver, CO", 
                "Boston, MA", "San Francisco, CA", "Portland, OR", "Bridgeport, CT", "Salt Lake City, UT", "Oklahoma City, OK",
                "Columbus, OH", "New Hampshire, NH", "Las Vegas, NV", "Billings, MT", "Portland, ME", "Sioux Falls, SD", 
                "Charleston, SC", "Nashville, TN", "Cheyenne, WY"],
    "size": list(range(10000, 60000, 5000)),
    "manager": ["John Doe", "Jane Smith", "Michael Johnson", "Emily Davis", "Christopher Brown",
            "Jessica Wilson", "Andrew Martinez", "Melissa Taylor", "Kevin Garcia", "Laura Miller"]
}