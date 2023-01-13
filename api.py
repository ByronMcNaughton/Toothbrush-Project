from flask import Flask, request
from flask_restful import Api, Resource
import sqlalchemy
from sqlalchemy import text

app = Flask(__name__)
api = Api(app)

##route total returns the total orders of all time, this can be filtered to a specific product, and grouped by product
@app.route("/total", methods=['GET'])
def totals():
    ##type represents toothbrush_type, valid parameters are all or type from database, any space should be substituted with an "_"
    type = request.args.get("type", None)
    group = request.args.get("group", None)

    if group not in ["true", "false", None]:
        return {"status": 422, "message": "Group must be one of True, False"}

    if type:
        type = str(type).lower().replace("_", " ")

    if group:
        group = str(group).lower()

    if type == "all" or type is None:
        sql = "SELECT count(*) from orders"
    else:
        sql = f"SELECT count(*) from orders WHERE toothbrush_type = '{type}'"

    out = {"status": 200}
    result = {}
    if group == "true":
        sql = sql.replace("count(*)", "toothbrush_type, count(*)")
        sql = sql + " GROUP BY toothbrush_type;"

        for row in sqlQuery(sql).fetchall():
            result[row[0]] = row[1]

    else:
        result["total"] = sqlQuery(sql+";").one()[0]

    out["result"] = result
    return out

#route sort shows the choice of whether orders are filtered by
@app.route("/sort", methods=["GET"])
def sort():
    type = request.args.get("type", None)
    if type not in ["day", "month", "year"]:
        return {"status": 422, "message": "type must be one of day, month, year"}
    out = {"status":200}
    if type == "day":
        res = sqlQuery('SELECT date_format(order_date, "%d") as order_day, COUNT(*) AS total from orders GROUP BY date_format(order_date, "%d") order by order_day;')
    elif type == "month":
        res = sqlQuery('SELECT date_format(order_date, "%m") as order_month, COUNT(*) AS total from orders GROUP BY date_format(order_date, "%m") order by order_month;')
    elif type == "year":
        res = sqlQuery('SELECT date_format(order_date, "%y") as order_year, COUNT(*) AS total from orders GROUP BY date_format(order_date, "%y") order by order_year;')

    result={}
    for row in res.fetchall():
        result[row[0]] = row[1]

    out["result"] = result
    return out

def sqlQuery(sql):
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://admin:{password}@toothbrush-sales.cldl8ux2pqs1.us-east-1.rds.amazonaws.com/Toothbrush")
    with engine.connect() as con:
        return (con.execute(text(sql)))


if __name__ == "__main__":
    app.run(debug=True)
