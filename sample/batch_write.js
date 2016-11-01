const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.batchWriteRow(config.tableName, [{
  key: {
    partitionKey: 'key_1',
    rowKey: 10001
  },
  columns: {
    'name': 'Small Big',
    'age': 31
  },
  action: 'PUT' // action default is PUT
}, {
  key: {
    partitionKey: 'key_1',
    rowKey: 10002
  },
  action: 'DELETE',
}, {
  key: {
    partitionKey: 'key_1',
    rowKey: 10003
  },
  action: 'DELETE',
  columns: ['name']
}], function(err, result, info){
  if (err) {
    console.warn(err);
  } else {
    console.log(result);
    console.log(info);
  }
});
