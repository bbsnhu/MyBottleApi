import json
import pymongo
from bson import json_util
from bottle import abort, delete, get, post, put, request, route, run


client = pymongo.MongoClient('localhost', 27017)
db = client['market']
collection = db['stocks']


@route('/stocks/api/v1.0/createStock/<stockTicker>', method='POST')
def post_create(stockTicker = None):
    try:
        if request.json and stockTicker:
            myDocument = {"Ticker" : stockTicker}
            myDocument.update(request.json)
            insResult = collection.insert_one(myDocument).acknowledged
            if insResult:
                return "Document Added"
            else:
                return "Document Insertion to DB Failed"
        else:
            return "Valid JSON Body and ticker path required"
    except pymongo.errors.ServerSelectionTimeoutError:
        abort(500, 'DB Connection Failed')
    finally:
        client.close()


@route('/stocks/api/v1.0/getStock/<stockTicker>', method='GET')
def get_read(stockTicker = None):
    try:
        search = {"Ticker":  '{}'.format(stockTicker)}
        # print(search)
        document = collection.find_one(search)
        if document is None:
            return "Ticker symbol not found\n"
        return json.dumps(document, indent=4, default=json_util.default)
    except pymongo.errors.ServerSelectionTimeoutError:
        abort(500, "DB Connection Failed")
    finally:
        client.close()


@route('/stocks/api/v1.0/updateStock/<stockTicker>', method="PUT")
def put_update(stockTicker):
    try:
        document = {"Ticker":  '{}'.format(stockTicker)}
        newDocument = request.json
        newDocumentWrap = {}
        newDocumentWrap.setdefault("$set", newDocument)
        collection.update_one(document, newDocumentWrap)
        return "Document Successfully Updated"
    except pymongo.errors.ServerSelectionTimeoutError:
        abort(500, "DB Connection Failed")
    finally:
        client.close()


@route('/stocks/api/v1.0/deleteStock/<stockTicker>', method="DELETE")
def delete_delete(stockTicker):
    try:    
        document = {"Ticker":  '{}'.format(stockTicker)}
        stat = collection.delete_one(document).deleted_count
        return "{} document(s) deleted".format(stat)
    except pymongo.errors.ServerSelectionTimeoutError:
        abort(500, "DB Connection Failed")
    finally:
        client.close()


#-------------------------------------------------------------------


@route('/stocks/api/v1.0/stockReport', method="GET")
def post_stockReport():
    userList = request.query.list.replace("[", "").replace("]", "").split(",")
    pipeSearch = [
        {"$match": {"Ticker": {"$in": userList}}},
        {"$project": {
            "_id": 0,
            "Ticker": 1, 
            "Sector": 1,
            "Industry":1,
            "Price": 1,
            "Change": 1
            }
        }
    ]
    results = list(db.stocks.aggregate(pipeSearch))
    return json.dumps(results, indent=4, default=json_util.default)


@route('/stocks/api/v1.0/industryReport/<industry>', method="GET")
def get_industryReport(industry = None):
    pipeSearch = [
        {"$match": {"Industry": industry}},
        {"$sort":{"Price": -1}},
        {"$limit": 5},
        {"$project" : {"_id" : 0, "Ticker" : 1, "Price" : 1}}
    ]
    results = list(db.stocks.aggregate(pipeSearch))
    return json.dumps(results, indent=4, default=json_util.default)


def main():
    """Main App Function - Entrypoint"""
    run(host='localhost', port=8080, reloader=True, debug=True)


if __name__ == "__main__":
    main()