import pandas as pd
from faker import Faker
import random


def generate_categories_data(fake):
    categories_data = pd.DataFrame({
        'id': range(1, 11),
        'category_name': [fake.word() for _ in range(10)],
        'category_description': [fake.sentence() for _ in range(10)]
    })
    return categories_data


def generate_customers_data(fake):
    customers_data = pd.DataFrame({
        'id': range(1, 101),
        'customer_name': [fake.company() for _ in range(100)],
        'industry': [fake.word() for _ in range(100)],
        'region': [fake.word() for _ in range(100)],
        'company_phone': [fake.phone_number() for _ in range(100)],
        'contact_email': [fake.company_email() for _ in range(100)],
        'contact_position': [fake.job() for _ in range(100)],
        'contact_name': [fake.name() for _ in range(100)],
        'department': [fake.word() for _ in range(100)]
    })
    return customers_data


def generate_event_types_data(fake):
    event_types_data = pd.DataFrame({
        'id': range(1, 6),
        'event_type_name': [fake.word() for _ in range(5)],
        'event_category_id': random.choices(range(1, 11), k=5),
        'event_description': [fake.sentence() for _ in range(5)],
        'event_priority': [random.choice(['High', 'Medium', 'Low']) for _ in range(5)],
        'created_by': [fake.name() for _ in range(5)],
        'created_at': [fake.date_time_this_year() for _ in range(5)]
    })
    return event_types_data


def main():
    fake = Faker('ko_KR')

    categories_data = generate_categories_data(fake)
    customers_data = generate_customers_data(fake)
    event_types_data = generate_event_types_data(fake)

    # 데이터 출력
    print("dim_categories 테이블 데이터:")
    print(categories_data)

    print("\ndim_customers 테이블 데이터:")
    print(customers_data)

    print("\ndim_event_types 테이블 데이터:")
    print(event_types_data)


if __name__ == '__main__':
    main()
