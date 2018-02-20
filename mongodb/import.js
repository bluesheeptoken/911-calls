var mongodb = require('mongodb');
var csv = require('csv-parser');
var fs = require('fs');

var MongoClient = mongodb.MongoClient;
var mongoUrl = 'mongodb://localhost:27017/911-calls';

var emptyToNull = function(str) {
  return (str && str.trim() !== "") ? str : null;
}

var getDate = function(str) {
  return new Date(str);
  /*return new Date(d.valueOf() + d.getTimezoneOffset() * 60000);*/
}

var buildLocation = function(data) {
  var lon = emptyToNull(data["lng"]);
  var lat = emptyToNull(data["lat"]);
  return lon && lat ? [parseFloat(lon), parseFloat(lat)] : null;
}

var insertCalls = function(db, callback) {
    var collection = db.collection('calls');

    var calls = [];
    fs.createReadStream('../911.csv')
        .pipe(csv())
        .on('data', data => {
            var call = {
                "location": buildLocation(data),
                "zip": parseFloat(data["zip"]),
                "title": (data["title"] !== undefined) ? data["title"].split(":")[0] : "",
                "cause": (data["title"] !== undefined) ? data["title"].split(":")[1] : "",
                "@timestamp": getDate(data["timeStamp"]),
                "twp": emptyToNull(data["twp"]),
                "addr": emptyToNull(data["addr"])
            };
            calls.push(call);
        })
        .on('end', () => {
            collection.insertMany(calls, (err, result) => {
                callback(result)
            });
        });
}

MongoClient.connect(mongoUrl, (err, db) => {
    insertCalls(db, result => {
        console.log(`${result.insertedCount} calls inserted`);
        db.close();
    });
});