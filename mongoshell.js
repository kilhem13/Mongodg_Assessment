// 1- print("Query 01")
// The highest salary of clerks
db.employees.aggregate([
    { $match: { "job": "clerk" } },
    { $group: { "_id": "$job", salary: { $max: "$salary" } } },
    { $project: { "_id": false } }
])

// 2- print("Query 02")
// The total salary of managers
db.employees.aggregate([
    { $match: { "job": "manager" } },
    { $group: { "_id": "$job", total: { $sum: "$salary" } } },
    { $project: { "_id": false } }
])

// 3- print("Query 03")
// The lowest, average and highest salary of the employees
db.employees.aggregate([
    { $group : { _id: null, max: { $max : "$salary" }, low: { $min : "$salary" }, avgSalary: { $avg: "$salary" }}
    },
    { $project: { "_id": false } }
]);

// 4- print("Query 04")
// The name of the departments
db.employees.aggregate([
    { $match: { department: { $exists: true } } },
    { $group: { _id: "$department.name" } },
    { $project: { _id: 0, name: "$_id" } }
])

// 5- print("Query 05")
// For each job: the job and the average salary for that job
db.employees.aggregate([
    { $group: { _id: "$job",  avgSalary: { $avg : "$salary" }}},
    { $project: { _id: 0, name: "$_id", avgSalary: "$avgSalary" } }
])
// 6- print("Query 06")
// For each department: its name, the number of employees and the average salary in that department
// (null departments excluded)
db.employees.aggregate([
    { $match: { department: { $exists: true } } },
    { $group: { _id: "$department.name", employees: { $sum: 1 }, avgSalary: { $avg: "$salary" } } },
    { $project: { _id: 0, name: "$_id", employees: 1, avgSalary: 1 } }
])

// 7- print("Query 07")
// The highest of the per-department average salary (null departments excluded)
db.employees.aggregate([
    { $match: { department: { $exists: true } } },
    { $group: { _id: "$department",  avgSalary: { $avg : "$salary" }}},
    { $sort: { "avgSalary": -1 } },
    { $limit: 1 },
    { $project: { _id: 0, max: "$avgSalary" } }
])
// 8- print("Query 08")
// The name of the departments with at least 5 employees (null departments excluded)
db.employees.aggregate([
    { $match: { department: { $exists: true } } },
    { $group: { _id: "$department.name", employees: { $sum: 1 } } },
    { $match: { employees: { $gte: 5 } } },
    { $project: { _id: 0, _name: "$_id" } }
])

// 9- print("Query 09")
// The cities where at least 2 missions took place
db.employees.aggregate([
    { $unwind: "$missions" },
    { $group: { _id: "$missions.location", missions: { $sum: 1 } } },
    { $match: { missions: { $gte: 2 } } },
    { $project: { _id: 0, city: "$_id" } }
])

// 10- print("Query 10")
// The highest salary
db.employees.aggregate([
    { $sort: { "salary": -1 } },
    { $limit: 1 },
    { $project: { _id: 0, salary: 1 } }
])

// 11- print("Query 11")
// The name of the departments with the highest average salary
db.employees.aggregate([
    { $match: { department: { $exists: true } } },
    { $group: { _id: "$department.name", avgSalary: { $avg: "$salary" } } },
    { $group: { _id: "$avgSalary", name: { $push: "$_id" } } },
    { $limit: 1 },
    { $unwind: "$name" },
    { $project: { _id: 0 } }
])

// 12- print("Query 12")
// For each city in which a mission took place, its name (output field "city") and the number of missions in that city
db.employees.aggregate([
    { $unwind: "$missions" },
    { $group: { _id: "$missions.location", missions: { $sum: 1 } } },
    { $project: { _id: 0, city: "$_id", missions: 1 } }
])

// 13- print("Query 13")
// The name of the departments with at most 5 employees
db.employees.aggregate([
    { $match: { department: { $exists: true } } },
    { $group: { _id: "$department.name", employees: { $sum: 1 } } },
    { $match: { employees: { $lte: 5 } } },
    { $project: { _id: 0, _name: "$_id" } }
])

// 14- print("Query 14")
// The average salary of analysts
db.employees.aggregate([
    { $group: { _id: "$job", avgSalary: { $avg: "$salary" } } },
    { $match: { _id: "analyst" } },
    { $project: { _id: 0 } }
])

// 15- print("Query 15")
// The lowest of the per-job average salary
db.employees.aggregate([
    { $group: { _id:"$job", lowest_avgSalary: { $avg: "$salary" } } },
    { $sort: {"lowest_avgSalary": 1 } },
    { $limit: 1},
    { $project: { _id: 0}}
])
// 16- print("Query 16")
// For each department: its name and the highest salary in that department
db.employees.aggregate([
    { $match: { department: { $exists: true } } },
    { $group: { _id: "$department.name", maxSalary: { $max: "$salary" } } },
    { $project: { _id: 0, name: "$_id", maxSalary: 1 } }
])

// 17- print("Query 17")
// The number of employees
db.employees.aggregate([
    { $group: { _id: null, nmbr_employees: {$sum:1} } },
    { $project: { _id: 0 } }
])
// 18- print("Query 18")
// One of the employees, with pretty printing (2 methods)
db.employees.find().limit(1).pretty()

// 19- print("Query 19")
// All the information about employees, except their salary, commission and missions
db.employees.aggregate([
    
    { $project: { salary: 0, missions: 0, commission: 0 } }
]).pretty()
// 20- print("Query 20")
// The name and salary of all the employees (without the field _id)
db.employees.aggregate([
    { $project: { _id: 0, name: "$name", salary: 1 } }
])
