var Kts = require('kts');

var client = new Kts({
  endpoint: '',
  accessKey: '',
  secretKey: ''
});
var tableName = '';

// list all tables
client.listTables(function onListTable(err, res, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(res);  // ['tableName1', 'tableName2' ...]
    console.log(info);  // { logId, consumedCapacity}
  }
});

// describe table
client.describeTable(tableName, function onDeleteTable(err, res, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(res);  // {...tableDescription}
    console.log(info);  // {logId, consumedCapacity}
  }
});

// put row
client.putRow(tableName, {
  key: {
    partitionKey: 'Key1'
    // rowKey: 'rk1',
  },
  columns: {
    'name': 'Jack',
    'age': 32,
    'weight': 56.3,
    'is_vip': false
  }
}, function onPutRow(err, res, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(res);  // undefined
    console.log(info);  // {logId, consumedCapacity}
  }
});

// get row
client.getRow(tableName, {
  key: {
    partitionKey: 'Key1',
  },
  columns: ['name', 'age'],  // default is null, get all columns
  strongConsistent: true,  // default is false
}, function (err, res, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(res);  // {key, columns}
    console.log(info);  // {logId, consumedCapacity}
  }
});

// scan
client.scan(tableName, {
  limit: 100,
  strongConsistent: true,
  startKey: {  // default is null, scan from first row
    partitionKey: 'Key1',
  },
  endKey: {  // default is null, scan until latest row
    partitionKey: 'Key9',
  }
}, function (err, res, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(res);  // [{...row1}, {...{row2}]
    console.log(info);  // {logId, consumedCapacity}
  }
});

// batch write rows
client.batchWriteRow(tableName, [{
  key: {
    partitionKey: 'Key2'
  },
  columns: {
    'name': 'Rose',
    'age': 31,
    'weight': 45.3,
    'is_vip': true
  }
}, {
  key: {
    partitionKey: 'Key3'
  },
  action: 'DELETE',
  columns: ['is_vip']
}, {
  key: {
    partitionKey: 'Key4'
  },
  action: 'DELETE'
}], function (err, res, info) {
  if (err) {
    console.log(err);
  } else {
    console.log(res);  // undefined
    console.log(info);  // {logId, consumedCapacity}
  }
});

// batch read rows
client.batchGetRow(tableName, [{
  partitionKey: 'Key2'
}, {
  partitionKey: 'Key3',
}], function (err, res, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log(res);  // [{...row1}, {...{row2}]
    console.log(info);  // {logId, consumedCapacity}
  }
});

console.log('end');
