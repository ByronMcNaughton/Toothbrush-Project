import sqlalchemy as db


def sql_to_dict(query):
    return_dict = {}
    for i in query:
        return_dict[i[0]] = i[1]
    return return_dict


def lambda_handler(event, context):
    #connect to database
    engine = db.create_engine("mysql+pymysql://admin:{password}@toothbrush-sales.cldl8ux2pqs1.us-east-1.rds.amazonaws.com/Toothbrush")
    con = engine.connect()

    #total orders
    full_total = con.execute('select count(*) from orders;').scalar()

    #total toothbrushes sold
    total_sold_brushes = int(con.execute('SELECT sum(order_quantity) from orders;').scalar())

    #total by toothbrush type
    type_total = con.execute('SELECT toothbrush_type ,count(*) as total from orders GROUP BY toothbrush_type;')
    type_total = sql_to_dict(type_total)

    #delivery status (Delivered / unsuccessful / in transit / Null)
    delivery_status = con.execute('SELECT delivery_status, count(*) as total from orders GROUP BY delivery_status;')
    delivery_status = sql_to_dict(delivery_status)

    #dispatch status
    dispatch_status = con.execute('SELECT dispatch_status, count(*) as total from orders GROUP BY dispatch_status;')
    dispatch_status = sql_to_dict(dispatch_status)

    #day / month / year
    orders_by_day = con.execute(r'SELECT date_format(order_date, "%%d") as order_day, COUNT(*) AS total from orders GROUP BY date_format(order_date, "%%d") order by order_day;')
    orders_by_day = sql_to_dict(orders_by_day)
    orders_by_month = con.execute(r'SELECT date_format(order_date, "%%m") as order_month, COUNT(*) AS total from orders GROUP BY date_format(order_date, "%%m") order by order_month;')
    orders_by_month = sql_to_dict(orders_by_month)
    orders_by_year = con.execute(r'SELECT date_format(order_date, "%%y") as order_year, COUNT(*) AS total from orders GROUP BY date_format(order_date, "%%y") order by order_year;')
    orders_by_year = sql_to_dict(orders_by_year)

    #top 5 postcodes
    top_postcodes = con.execute('SELECT postcode.postcode, count(*) as total from orders join postcode on orders.delivery_postcode = postcode.id GROUP BY delivery_postcode ORDER BY total DESC limit 5;')
    top_postcodes = sql_to_dict(top_postcodes)

    #delivery = billing
    bill_match_deliv = con.execute('SELECT count(*) as matching_postcodes from orders where delivery_postcode = billing_postcode;').scalar()

    #age
    age = con.execute('SELECT customer_age, count(*) as total from orders GROUP BY customer_age ORDER BY customer_age;')
    age = sql_to_dict(age)


    return{
        "full_total": full_total,
        "total_sold_brushes": total_sold_brushes,
        "type_total": type_total,
        "delivery_status": delivery_status,
        "dispatch_status": dispatch_status,
        "orders_by_day": orders_by_day,
        "orders_by_month": orders_by_month,
        "orders_by_year": orders_by_year,
        "top_postcodes": top_postcodes,
        "bill_match_deliv": bill_match_deliv,
        "ages": age
    }