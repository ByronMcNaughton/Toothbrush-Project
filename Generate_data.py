import json
import random
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime as dt



#Info for database
db_username = "admin"
db_password = "{password}"
db_host = "toothbrush-sales.cldl8ux2pqs1.us-east-1.rds.amazonaws.com"
db_name = "Toothbrush"

def main():
    #connect to database
    engine = db.create_engine("mysql+pymysql://admin:{password}@toothbrush-sales.cldl8ux2pqs1.us-east-1.rds.amazonaws.com/Toothbrush")

    todays_date = pd.to_datetime(dt.date.today()).date()
    null_df = None

    #if database does not have table "orders", assuming that if orders table has been created we are doing a full dump
    insp = db.inspect(engine)
    if not insp.has_table("orders"):
        first_order_date = pd.to_datetime('2022-01-01')
        next_id = 1

    else:
        first_order_date = pd.read_sql_query('SELECT order_date FROM orders ORDER BY order_date DESC limit 1;', engine)
        first_order_date = first_order_date.loc[0, "order_date"].date()+ dt.timedelta(days=1)

        next_id = pd.read_sql_query('SELECT MAX(order_number) FROM orders;', engine)
        next_id = int(next_id.loc[0, "MAX(order_number)"]) + 1

        null_df = pd.read_sql_query('SELECT * FROM null_orders;', engine)

    #check how may postcodes are stored
    max_postcode_id_sql = pd.read_sql_query('SELECT MAX(id) FROM postcode;', engine)
    max_pc = max_postcode_id_sql.loc[0, "MAX(id)"]


    while first_order_date <= todays_date:
        #run program - to implement

        if null_df is not None and not null_df.empty:
            null_df = update_delivery_columns(null_df)

        n = np.random.choice(range(50, 150))
        df = generate_order_number(next_id, next_id + n)
        df = add_columns(df, first_order_date, n, max_pc)
        df = add_delivery_columns(df, n)

        if null_df is None:
            df.to_sql('orders', engine, if_exists='replace', index=False)

        df = pd.concat([df, null_df], ignore_index=True)
        null_df = df[df['delivery_date'].isnull()]

        df.to_sql('todays_orders', engine, if_exists='replace', index=False)
        null_df.to_sql('null_orders', engine, if_exists='replace', index=False)

        sql_table_update = "INSERT INTO orders " \
                           "SELECT " \
                           "todays_orders.order_number, " \
                           "todays_orders.toothbrush_type, " \
                           "todays_orders.order_date, " \
                           "todays_orders.customer_age, " \
                           "todays_orders.order_quantity, " \
                           "todays_orders.delivery_postcode, " \
                           "todays_orders.billing_postcode, " \
                           "todays_orders.dispatch_status, " \
                           "todays_orders.dispatch_date, " \
                           "todays_orders.delivery_status, " \
                           "todays_orders.delivery_date " \
                           "FROM todays_orders " \
                           "ON DUPLICATE KEY UPDATE " \
                           "orders.order_number = todays_orders.order_number, " \
                           "orders.toothbrush_type = todays_orders.toothbrush_type, " \
                           "orders.order_date = todays_orders.order_date, " \
                           "orders.customer_age = todays_orders.customer_age, " \
                           "orders.order_quantity = todays_orders.order_quantity, " \
                           "orders.delivery_postcode = todays_orders.delivery_postcode, " \
                           "orders.billing_postcode = todays_orders.billing_postcode, " \
                           "orders.dispatch_status = todays_orders.dispatch_status, " \
                           "orders.dispatch_date = todays_orders.dispatch_date, " \
                           "orders.delivery_status = todays_orders.delivery_status, " \
                           "orders.delivery_date = todays_orders.delivery_date;"

        with engine.connect() as con:
            con.execute(sql_table_update)

        print("Updated Tables - "+str(first_order_date))

        first_order_date = first_order_date + dt.timedelta(days=1)
        next_id = next_id + n

def update_delivery_columns(df):
    # orders that weren't dispatched in the first generation, are updated to dispatch
    df.loc[(df['dispatch_status'] != 'Dispatched'), 'dispatch_status'] = 'Dispatched'

    n = df.shape[0]
    # generate time intervals
    order_received = np.random.normal(0.2, 0.01, n)
    order_confirmed = np.random.normal(0.9, 0.2, n)
    order_dispatched = np.random.normal(6, 0.5, n)

    # add dispatch time
    df.loc[df['dispatch_status'] == 'Dispatched', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received + order_confirmed + order_dispatched, unit='h'))

    delivery_status_transit = ['Delivered', 'Unsuccessful']

    # update delivery_status for old data
    null_dispatch_mask_1 = (df['delivery_status'] == 'In Transit') & (df['dispatch_date'].dt.hour <= 4)
    df.loc[null_dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status_transit, p=[0.8, 0.2])
    null_dispatch_mask_2 = (df['delivery_status'] == 'In Transit') & (df['dispatch_date'].dt.hour > 4)
    df.loc[null_dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status_transit, p=[0.99, 0.01])

    # add delivery_status to generate insight re: unsuccessful deliveries before 4am
    delivery_status = ['In Transit', 'Delivered', 'Unsuccessful']

    dispatch_mask_1 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status, p=[0.4, 0.2, 0.4])

    dispatch_mask_2 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status, p=[0.3, 0.69, 0.01])

    # generate time intervals
    in_transit = np.random.normal(1, 0.2, n)
    delivered = np.random.normal(26, 4, n)
    unsuccessful = np.random.normal(26, 8, n)

    # generate delivery time
    df.loc[df['delivery_status'] == 'In Transit', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit, unit='h'))
    df.loc[df['delivery_status'] == 'Delivered', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + delivered, unit='h'))
    df.loc[df['delivery_status'] == 'Unsuccessful', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + unsuccessful, unit='h'))
    return df

def generate_order_number(l, n):
    lst = []
    start = l
    for i in range(l, n):
        lst.append(''.join(['{0:08}'.format(start)]))
        start += 1
    df = pd.DataFrame({'order_number': list(set(lst))})

    return df

def add_columns(df, date, n, max_pc):
    # add two types of toothbrushes
    toothbrush_type = ['Toothbrush 2000', 'Toothbrush 4000']
    df['toothbrush_type'] = np.random.choice(toothbrush_type, size=n)

    tooth_1 = (df['toothbrush_type'] == 'Toothbrush 2000')
    tooth_2 = (df['toothbrush_type'] == 'Toothbrush 4000')

    len_tooth_1 = df[tooth_1].shape[0]
    len_tooth_2 = df[tooth_2].shape[0]

    # add random times
    times = []
    for i in range(n):
        times.append(date + dt.timedelta(seconds=random.randrange(86400)))
    df['order_date'] = pd.to_datetime(times)
    df['order_date'] = pd.to_datetime(df['order_date'])


    # adding in insight: re age of orderer and toothbrush_type
    age_1 = np.random.randint(11, 75, len_tooth_1)
    age_2 = np.random.randint(9, 26, len_tooth_2)

    df.loc[tooth_1, 'customer_age'] = age_1
    df.loc[tooth_2, 'customer_age'] = age_2

    df['customer_age'] = df['customer_age'].astype(int)

    # adding quantity
    df['order_quantity'] = np.random.choice(range(1, 10), n)


    # randomly choosing postcodes
    df['delivery_postcode'] = list(random.sample(range(1, max_pc), n))
    # setting the billing_postcode as the delivery_postcode
    df['billing_postcode'] = df['delivery_postcode']

    # randomly picking the number of records where the billing and delivery_postcode are different
    postcode_split = np.random.choice(range(1, int(n / 2)), 1)[0]
    # randomly picking a different billing_postcode
    df.loc[:postcode_split - 1, 'billing_postcode'] = list(random.sample(range(1, max_pc), postcode_split))

    return df

def add_delivery_columns(df, n):
    days_ago = dt.date.today() - dt.timedelta(days=3)

    # add dispatch_status
    dispatch_status = ['Order Received', 'Order Confirmed', 'Dispatched']
    df['dispatch_status'] = np.random.choice(dispatch_status, size=n)

    # all orders have been dispatched for first run
    df.loc[(df['order_date'].dt.date < days_ago), 'dispatch_status'] = 'Dispatched'

    # generate time intervals
    order_received = np.random.normal(0.2, 0.01, n)
    order_confirmed = np.random.normal(0.9, 0.2, n)
    order_dispatched = np.random.normal(6, 0.5, n)

    # generate dispatch time
    df.loc[df['dispatch_status'] == 'Order Received', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received, unit='h'))
    df.loc[df['dispatch_status'] == 'Order Confirmed', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received + order_confirmed, unit='h'))
    df.loc[df['dispatch_status'] == 'Dispatched', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received + order_confirmed + order_dispatched, unit='h'))

    # add delivery_status to generate insight re: unsuccessful deliveries before 4am
    delivery_status = ['In Transit', 'Delivered', 'Unsuccessful']

    dispatch_mask_1 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status, p=[0.4, 0.2, 0.4])

    dispatch_mask_2 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status, p=[0.3, 0.69, 0.01])

    # forcing all old orders to have some delivery data
    delivery_status = ['Delivered', 'Unsuccessful']
    dispatch_mask_1 = (df['order_date'].dt.date < days_ago) & (df['dispatch_date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status, p=[0.8, 0.2])

    dispatch_mask_2 = (df['order_date'].dt.date < days_ago) & (df['dispatch_date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status, p=[0.99, 0.01])

    # generate time intervals
    in_transit = np.random.normal(1, 0.2, n)
    delivered = np.random.normal(26, 4, n)
    unsuccessful = np.random.normal(26, 8, n)

    # generate delivery time
    df.loc[df['delivery_status'] == 'In Transit', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit, unit='h'))
    df.loc[df['delivery_status'] == 'Delivered', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + delivered, unit='h'))
    df.loc[df['delivery_status'] == 'Unsuccessful', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + unsuccessful, unit='h'))

    return df


def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Complete')
    }