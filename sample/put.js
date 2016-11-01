const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.putRow(config.tableName, {
  key: {
    partitionKey: 'key_1',
    rowKey: 10003
  },
  columns: {
    'name': 'Jack',  // string
    'age': 2147483649,  // int64
    'weight': 62.3,  // double
    'is_vip': true  // bool
  }
}, function(err, result, info){
  if (err)
    throw err;
  console.log(result);
  console.log(info);
});