use('stocksDB')
// db.getCollection('rawNewsColl').find({}).limit(100).toArray()

db.getCollectionNames().forEach(function(collName) {
    if(collName.endsWith("_mktCap")) {
        print("Dropping: " + collName);
        db[collName].drop();
    }
})

db.getCollectionNames().forEach(function(collName) {
    if(collName.endsWith("_empCt")) {
        print("Dropping: " + collName);
        db[collName].drop();
    }
})

db.getCollectionNames().forEach(function(collName) {
    if(collName.endsWith("_execComp")) {
        print("Dropping: " + collName);
        db[collName].drop();
    }
})

db.getCollectionNames().forEach(function(collName) {
    if(collName.endsWith("_grade")) {
        print("Dropping: " + collName);
        db[collName].drop();
    }
})