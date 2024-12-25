use('stocksDB')
db.getCollection('rawNewsColl').find({}).limit(100).toArray()