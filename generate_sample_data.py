from faker import Faker
import pandas as pd
import random
import faker_commerce

fake = Faker()
fake.add_provider(faker_commerce.Provider)

#For Spark projects
NUM_USERS = 100
NUM_ORDERS = 200

# Generate Users Data
users = []
for user_id in range(1, NUM_USERS + 1):
    users.append({
        "user_id": user_id,
        "name": fake.name() if random.random() > 0.1 else None,
        "email": fake.email() if random.random() > 0.1 else None,
        "city": fake.city(),
        "state": fake.state(),
        "zip_code": fake.zipcode(),
        "country": fake.country(),
        "signup_date": fake.date_between(start_date="-1y", end_date="today"),
    })

# Create duplicates (copy first 5 users)
users.extend(users[:5])

users_df = pd.DataFrame(users)
users_df.to_csv('source_data/users_data.csv', index=False)

# Generate Orders Data

orders = []

for order_id in range(1, NUM_ORDERS + 1):
    orders.append({
        "user_id": random.randint(1, NUM_USERS),
        "order_id": order_id,
        "product": fake.ecommerce_name(),
        "category": fake.ecommerce_category(),
        "price": random.randint(100, 2000),
        "quantity": fake.random_int(1, 5) if random.random() > 0.05 else None,
        "order_date": fake.date_between(start_date="-1y", end_date="today"),
    })

# Create duplicate orders
orders.extend(orders[:10])
orders_df = pd.DataFrame(orders)
orders_df.to_csv('source_data/orders_data.csv', index=False)

