/* global use, db */
// MongoDB Playground
// To disable this template go to Settings | MongoDB | Use Default Template For Playground.
// Make sure you are connected to enable completions and to be able to run a playground.
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.
// The result of the last command run in a playground is shown on the results panel.
// By default the first 20 documents will be returned with a cursor.
// Use 'console.log()' to print to the debug output.
// For more documentation on playgrounds please refer to
// https://www.mongodb.com/docs/mongodb-vscode/playgrounds/

// Select the database to use.
use('stocksDB');

// Insert a few documents into the sales collection.
//db.getCollection('rawPriceColl').find({}).sort({timestamp: 1}).limit(1).toArray()

// delete all documents from the collection
//db.getCollection('rawPriceColl').deleteMany({});

// db('stocksDB').collection('rawPriceColl').aggregate([
//     {
//         $addFields: {
//             year: { $year: "$timestamp" }
//         }
//     },
//     {
//         $match: {
//             year: 2016
//         }
//     },
//     {
//         $sort: { "timestamp": 1 }
//     },
//     {
//         $skip: 154900
//     },
//     {
//         $limit: 100
//     }
// ]).toArray()

use('stocksDB')
//db.getCollection('rawPriceColl').distinct('ticker')
// db.rawPriceColl.aggregate([
//     { $match: { ticker: 'AAPL' } }, // Filter documents where ticker is 'AAPL'
//     { $group: { _id: "$timestamp" } }, // Group by timestamp to get distinct values
//     { $project: { timestamp: "$_id", _id: 0 } } // Project the distinct timestamps
// ])

// db.getCollection('rawPriceColl').deleteMany({
//     ticker: { $in: ["CRWD", "PANW", "AAPL", "MSFT"] }
// });

db.getCollection('rawNewsColl').find({}).limit(100).toArray();