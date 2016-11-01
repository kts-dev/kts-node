const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.batchGetRow(config.tableName, [{
  partitionKey: 'key_1',
  rowKey: 10001
}, {
  partitionKey: 'key_1',
  rowKey: 10003
}], {
  columns: ['name'],  // default get all columns
  strongConsistent: true
}, function (err, result, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(result);
    console.log(info);
  }
});