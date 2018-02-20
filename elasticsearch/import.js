var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error',
  requestTimeout: Infinity,
  keepAlive: false
});

var emptyToNull = function(str) {
  return (str && str.trim() !== "") ? str : null;
}

var buildLocation = function(data) {
  var lon = emptyToNull(data["lat"]);
  var lat = emptyToNull(data["lng"]);
  return lon && lat ? [parseFloat(lon), parseFloat(lat)] : [0.0, 0.0];
}

let calls = [];

fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      calls.push({ "index" : { "_index" : "call", "_type" : "call" } });
      var call = {
          "location" : buildLocation(data),
          "zip": parseFloat(data["zip"]),
          "title" : (data["title"] !== undefined) ? data["title"].split(":")[0]: "",
          "cause" : (data["title"] !== undefined) ? data["title"].split(":")[1]: "",
          "@timestamp": new Date(data["timeStamp"]),
          "twp": emptyToNull(data["twp"]),
          "addr": emptyToNull(data["addr"])
      };
      calls.push(call);
    })
    .on('end', () => {
      esClient.bulk({
        body: calls
      }, (err, resp) => {
        if (err) { throw err; }
        console.log(`${resp.items.length} calls inserted`);
      });
    });
