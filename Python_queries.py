#1
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

#2
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$match": { "job": "manager" } },
    { "$group": { "_id": "$job", "total": { "$sum": "$salary" } } },
    { "$project": { "_id": 0 } }
])

for elem in result:
    print(elem)

#3
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient.company
mycol = mydb["employees"]
pipeline = [
    { "$group" : { "_id": "null", "max": { "$max" : "$salary" }, "low": { "$min" : "$salary" }, "avgSalary": { "$avg": "$salary" }}
    },
    { "$project": { "_id": 0 } }
            ]
list(mydb.employees.aggregate(pipeline))