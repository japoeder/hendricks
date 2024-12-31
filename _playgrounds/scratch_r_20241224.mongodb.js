use('stocksDB')
// db.getCollection('rawNewsColl').find({}).limit(100).toArray()

db.getCollectionNames().forEach(function(collName) {
    if(collName.endsWith("_mktCap")) {
        print("Dropping: " + collName);
        db[collName].drop();
    }
})