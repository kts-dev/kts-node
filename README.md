# kts-node

node.js下的KTS的SDK客户端

## 安装

### 安装 npm（node package manager）

```bash
$ curl http://npmjs.org/install.sh | sh
```

### 安装 kts-node

```bash
$ npm install kts
```

## 使用方法

### 建立kts连接

```javascript
const Kts = require('kts');
var client = new Kts({
  endpoint: 'http://bj-region1-kts.ksyun.com',  // just demo
  accessKey: 'KTSAccessKey',
  secretKey: 'KTSSecretKey',
});
```

其中`endpoint`为kts服务器地址，`accessKey`和`secretKey`为访问的密钥，在创建kts服务之后可以在控制台查询到这些信息。

### 通用信息

#### 回调函数

API接口中需要传入的回调函数一般形式为`function(err, result, info)`，其中：

* 参数`err`表示错误信息，可以使用该参数判断是否请求成功，如果成功则`err`为空，否则为错误的具体信息
* 参数`result`为本次请求的返回数据
* 参数`info`为请求附带的辅助信息，包括：
  * logId：本次请求的随机编号，一般用于请求跟踪查询
  * consumedCapacity：本次请求消耗的吞吐量

具体参数提供的属性以及作用可以查看对应API的详细信息。

注：为了简洁引导，示例代码中均省略部分参数。

### API列表

- [Create Table](#CreateTable)
- [List Tables](#ListTables)
- [Describe Table](#DescribeTable)
- [Delete Table](#DeleteTable)
- [Update Table](#UpdateTable)
- [Put Row](#PutRow)
- [Get Row](#GetRow)
- [Delete Row](#DeleteRow)
- [Scan](#Scan)
- [Batch Get Row](#BatchGetRow)
- [Batch Write Row](#BatchWriteRow)

<a name='CreateTable' ></a> 

### Create Table

在您账号下创建一张表

```javascript
client.createTable({
  tableName: 'tableName',
  schema: {
    partitionKeyType: "STRING",  // [STRING, INT64]
    rowKeyType: "INT64"  // or null
  },
  provisionedThroughput: {
    readCapacityUnits: 10,
    writeCapacityUnits: 10
  }
}, function(err) {
  if (err)
    console.log(err);
});
```

<a name='ListTables'> </a>

### List Tables

列举您账号下所有的表名。

```javascript
client.listTables(function(err, tableNames) {
  if (err)
    throw err;
  console.log(tableNames);
});
```

<a name='DescribeTable'> </a>

### Describe Table

获得指定表的定义以及状态信息。

```javascript
client.describeTable('tableName', function(err, tableDescription) {
  if (err)
    throw err;
  console.log(tableDescription);
});
```

<a name='DeleteTable'> </a>

### Delete Table

删除指定表。

```javascript
client.deleteTable('tableName', function(err) {
  if (err)
    console.log(err);
});
```

<a name='UpdateTable'> </a>

### Update Table

修改表的吞吐配置。

```javascript
client.updateTable('tableName', {
  readCapacityUnits: 10,
  writeCapacityUnits: 10
}, function(err) {
  if (err)
    console.log(err);
});
```

<a name='PutRow'> </a>

### Put Row

插入一行数据，如果指定key的数据存在则会合并和覆盖。

```javascript
client.putRow('tableName', {
  key: {
    partitionKey: 'key_1',
    rowKey: 10001
  },
  columns: {
    'name': 'Rose',
    'age': 28,
    'weight': 51.0
  }
}, function(err) {
  if (err)
    console.log(err);
});
```

<a name='GetRow'></a>

### Get Row

指定key读取一行数据。

```javascript
client.getRow('tableName', {
  partitionKey: 'key_1',
  rowKey: 10001,
  strongConsistent: true  // 默认为false, 表示非强一致性读
}, function(err, row) {
  if (err)
    throw err;
  console.log(row);
});
```

<a name='DeleteRow'> </a>

### Delete Row

删除一行数据。

```javascript
client.deleteRow('tableName', {
  partitionKey: 'key_1',
  rowKey: 10001
}, function(err) {
  if (err)
    console.log(err);
});
```

<a name='Scan'> </a>

### Scan

顺序读取一批数据，如果没有指定开始key则从第一条数据开始扫描。

```javascript
// 方式1
client.scan('tableName', {
  limit: 100,
  startKey: {
    partitionKey: 'key_00',
    rowKey: 10001
  },
  endKey: {
    partitionKey: 'key_99',
    rowKey: 99999
  }
}, function(err, rows, next_key) {
  if (err)
    throw err;
  console.log(rows);
});

// 方式2.
client.scan('tableName', function(err, rows, info) {
  if (err)
    throw err;
  console.log(rows);
  if (info.nextStartKey)
    // 需要继续扫描，下次位置为info.nextStartKey
});
```

如果一次请求未能将指定范围的数据全部返回，则会将下一行数据的起始位置（该位置不一定存在数据）赋值给`info.nextStartKey`返回；反之，如果全部数据都已经获取完毕，则该值为null。

<a name='BatchGetRow'> </a>

### BatchGetRow

指定key的列表批量查询一批数据。

```javascript
client.batchGetRow('tableName', [{
  partitionKey: 'key_1',
  rowKey: 10001,
  strongConsistent: true
}, {
  partitionKey: 'key_1',
  rowKey: 10002
}], function(err, rows) {
  if (err)
    throw err;
  console.log(rows);
})
```

注意：如果一次读取因为吞吐或其他限制导致未完成所有行的读取，则仅返回成功读取的行（行数据保证完成）。

<a name='BatchWriteRow'> </a>

### Batch Write Row

批量写入一批数据。

```javascript
client.batchWriteRow('tableName', [{
  // 插入一行数据
  key: {
    partitionKey: 'key_1',
    rowKey: 10001
  },
  columns: {
    'name': 'Rose',
    'age': 28
  }
}, {
  // 删除指定列
  key: {
    partitionKey: 'key_1',
    rowKey: 10002
  },
  action: 'DELETE',
  columns: ['name']
}, {
  // 删除一行
  key: {
    partitionKey: 'key_2',
    rowKey: 10003
  },
  action: 'DELETE'
}], function(err) {
  if (err)
    throw err;
});
```

**Batch Write Row** 支持批量的插入、修改以及删除命令，不支持在单个Batch命令中多次修改同一行数据。其中，与**Put Row**一致，如果插入的行已经在kts数据库中存在，则会合并插入的数据；如果插入的列已经存在则会被该次操作的列值覆盖。
注意一次batch命令中不能对同一行执行多次操作。
