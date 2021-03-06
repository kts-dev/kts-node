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

// filter case
var single = {
  filterType: filter.FilterType.SingleColumnValueFilter,
  compare: {
    column: {age: 214748349},
    compareType: filter.CompareType.EQUAL
  }
};

var myfilter = {
  filterType: filter.FilterType.FilterList,
  filterListType: filter.FilterListType.MUST_PASS_ONE,
  filters: [
    {
      filterType: filter.FilterType.SingleColumnValueFilter,
      compare: {
        column: {age: 214748349},
        compareType: filter.CompareType.EQUAL
      }
    },
    {
      filterType: filter.FilterType.SingleColumnValueFilter,
      compare: {
        column: {age: 2147483649},
        compareType: filter.CompareType.EQUAL
      }
    }
  ]
};

client.getRow(config.tableName, {
  key: {
    partitionKey: 'key_1',
    rowKey: 10001
  },
  strongConsistent: false,  // optional, default is false
  filter: myfilter
}, function(err, result, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log("getrow");
    console.log(result);
    console.log(info);
  }
});

client.scan(config.tableName, {
  'startKey': {
    partitionKey: 'astart_partition_key',
    rowkey: 10001
  },
  filter: myfilter
}, function(err, result, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log("scan");
    console.log(result);
    console.log(info);
  }
});


client.batchGetRow(config.tableName, [{
  partitionKey: 'key_1',
  rowKey: 10001
}], {
  columns: ['name'],  // default get all columns
  strongConsistent: true,
  filter: myfilter
}, function(err, result, info) {
  if (err) {
    console.warn(err);
  } else {
    console.log("batchGetRow");
    console.log(result);
    console.log(info);
  }
});

console.log('end');
