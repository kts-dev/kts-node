const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.deleteRow(config.tableName, {
    partitionKey: 'key_1',
    rowKey: 10001
  }, function (err, result, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(result); // will null
    console.log(info);
  }
});