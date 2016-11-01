const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.scan(config.tableName, {'startKey': {partitionKey: 'start_partition_key', rowKey:'start_row_key'}}, function (err, result, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(result);
    console.log(info);
  }
});