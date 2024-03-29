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
#4
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$match": { "department": { "$exists": 1 } } },
    { "$group": { "_id": "$department.name" } },
    { "$project": { "_id": 0, "name": "$_id" } }
])
for elem in result:
    print(elem)
#5
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
       { "$group": { "_id": "$job",  "avgSalary": { "$avg": "$salary" }}},
        { "$project": { "_id": 0, "name": "$_id", "avgSalary": "$avgSalary" } }
])
for elem in result:
    print(elem)
#6
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$match": { "department": { "$exists": 1 } } },
    { "$group": { "_id": "$department.name", "employees": { "$sum": 1 }, "avgSalary": { "$avg": "$salary" } } },
    { "$project": { "_id": 0, "name": "$_id", "employees": 1, "avgSalary": 1 } }
])
for elem in result:
    print(elem)

#7
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
       { "$match": { "department": { "$exists": 1 } } },
        { "$group": { "_id": "$department",  "avgSalary": { "$avg" : "$salary" }}},
        { "$sort": { "avgSalary": -1 } },
        { "$limit": 1 },
        { "$project": { "_id": 0, "max": "$avgSalary" } }
])
for elem in result:
    print(elem)
#8
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$match": { "department": { "$exists": 1 } } },
    { "$group": { "_id": "$department.name", "employees": { "$sum": 1 } } },
    { "$match": { "employees": { "$gte": 5 } } },
    { "$project": { "_id": 0, "name": "$_id" } }
])
for elem in result:
    print(elem)


#9
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
          { "$unwind": "$missions" },
    { "$group": { "_id": "$missions.location", "missions": { "$sum": 1 } } },
    { "$match": { "missions": { "$gte": 2 } } },
    { "$project": { "_id": 0, "city": "$_id" } }

])

for elem in result:
    print(elem)

#10
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$sort": { "salary": -1 } },
    { "$limit": 1 },
    { "$project": { "_id": 0, "salary": 1 } }
])
for elem in result:
    print(elem)


#11
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
          { "$match": { "department": { "$exists": 1 } } },
    { "$group": { "_id": "$department.name", "avgSalary": { "$avg": "$salary" } } },
    { "$group": { "_id": "$avgSalary", "name": { "$push": "$_id" } } },
    { "$limit": 1 },
    { "$unwind": "$name" },
    { "$project": { "_id": 0 } }

])

for elem in result:
    print(elem)

#12
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$unwind": "$missions" },
    { "$group": { "_id": "$missions.location", "missions": { "$sum": 1 } } },
    { "$project": { "_id": 0, "city": "$_id", "missions": 1 } }
])
for elem in result:
    print(elem)


#13
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$match": { "department": { "$exists": 1 } } },
    { "$group": { "_id": "$department.name", "employees": { "$sum": 1 } } },
    { "$match": { "employees": { "$lte": 5 } } },
    { "$project": { "_id": 0, "_name": "$_id" } }
])

for elem in result:
    print(elem)
#14
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$group": { "_id": "$job", "avgSalary": { "$avg": "$salary" } } },
    { "$match": { "_id": "analyst" } },
    { "$project": { "_id": 0 } }
])
for elem in result:
    print(elem)


#15
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$group": { "_id":"$job", "lowest_avgSalary": { "$avg": "$salary" } } },
    { "$sort": {"lowest_avgSalary": 1 } },
    { "$limit": 1},
    { "$project": { "_id": 0}}
])
for elem in result:
    print(elem)


#16
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$match": { "department": { "$exists": 1 } } },
    { "$group": { "_id": "$department.name", "maxSalary": { "$max": "$salary" } } },
    { "$project": { "_id": 0, "name": "$_id", "maxSalary": 1 } }
])
for elem in result:
    print(elem)



#17
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$group": { "_id": "null", "nmbr_employees": {"$sum":1} } },
    { "$project": { "_id": 0 } }
])
for elem in result:
    print(elem)

#18
import pymongo
import pprint
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
pprint.pprint(db.employees.find_one())

#19
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
{ "$project": { "salary": 0, "missions": 0, "commission": 0 } }
])
for elem in result:
    print(elem)

#20
import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient.company
result = db.employees.aggregate([
    { "$project": { "_id": 0, "name": "$name", "salary": 1 } }
])
for elem in result:
    print(elem)