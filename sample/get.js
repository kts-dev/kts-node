const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.getRow(config.tableName, {
  key: {
    partitionKey: 'key_1',
    rowKey: 10001
  },
  strongConsistent: true  // optional, default is false
}, function(err, result, info){
  if (err) {
    console.warn(err);
  } else {
    console.log(result);
    console.log(info);
  }
});
