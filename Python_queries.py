import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient.company
mycol = mydb["employees"]
pipeline = [
    { "$match": { "job": "clerk" } },
    { "$group": { "_id": "$job", "salary": { "$max": "$salary" } } },
    { "$project": { "_id": 0 } }
            ]
list(mydb.employees.aggregate(pipeline))