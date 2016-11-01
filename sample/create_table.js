const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.createTable(config.tableName, {
  schema: {
    partitionKeyType: 'STRING',
    rowKeyType: 'INT64'
  },
  provisionedThroughput: {
    readCapacityUnits: 100,
    writeCapacityUnits: 100
  }
}, function(err, result, info){
  if (err) {
    console.warn(err);
  } else {
    console.log(result);
    console.log(info);
  }
});