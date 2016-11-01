const Kts = require('../lib/kts.js');

const config = require('./config.js');

var client = new Kts({
  endpoint: config.endpoint,
  accessKey: config.accessKey,
  secretKey: config.secretKey
});

client.deleteTable(config.tableName, function (err, result, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(result);
    console.log(info);
  }
});