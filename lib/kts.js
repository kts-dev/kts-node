'use strict';

const Http = require('http');
const Url = require('url');
const BufferHelper = require('bufferhelper');

const Proto = require('./proto.js');
const Signer = require('./signer.js');

const Utils = {
  getDateTime: function () {
    return new Date().toISOString();
  }
};

function Client(options) {
  this.endpoint = options.endpoint;
  this.accessKey = options.accessKey;
  this.secretKey = options.secretKey;
  this.xVersion = options.xVersion || '2016-03-01';

  var url = Url.parse(options.endpoint);
  this.hostname = url.hostname;
  this.port = url.port;

  this.signer = new Signer({
    accessKey: this.accessKey,
    secretKey: this.secretKey
  });
  this.agent = new Http.Agent({KeepAlive: true});
}
/**
 * internal request
 * @param action {{name, serialize, decode, unserialize, getInfo}}
 * @param param
 * @param callback
 */
Client.prototype.request = function (action, param, callback) {
  var buff = action.serialize(param);
  var http_options = {
    host: this.hostname,
    port: this.port,
    path: '/TableService.' + action.name,
    method: 'POST',
    headers: {
      'Content-Length': buff.byteLength.toString(),
      'Connection': 'Keep-Alive',
      'X-ACTION': action.name,
      'X-VERSION': this.xVersion,
      'X-Kws-Date': Utils.getDateTime(),
      'Content-Type': 'application/binary'
    },
    agent: this.agent
  };
  http_options.headers['Authorization'] = this.signer.sign(http_options.method, http_options.headers);

  var req = Http.request(http_options, function (res) {
    res.on("error", function (err) {
      callback(err);
    });

    var body = new BufferHelper();
    res.on("data", function (data) {
      body.concat(data);
    });

    res.on("end", function(){
      try {
        var pb = action.decode(body.toBuffer());
        Proto.checkResponse(pb);
        var result = action.unserialize(pb);
        var info = action.getInfo(pb);
      } catch (err) {
        return callback(err);
      }
      callback(null, result, info);
    });
  });

  req.on("error", function (err) {
    callback(err);
  });

  req.write(buff);
  req.end();
};
/**
 * create table
 * @param tableName {string}
 * @param options{{
 * schema:{partitionKeyType, rowKeyType},
 * provisionedThroughput: {readCapacityUnits, writeCapacityUnits},
 * }}
 * @param callback {function(err)}
 */
Client.prototype.createTable = function (tableName, options, callback) {
  if (!tableName){
    throw 'invalid table name';
  }

  var params = {tableName: tableName};
  options = options || {};
  try {
    params.schema = options.schema || {
        partitionKeyType: "STRING",
        rowKeyType: null
      };
    params.provisionedThroughput = options.provisionedThroughput || {
        readCapacityUnits: 10,
        writeCapacityUnits: 10
      };
  } catch (err) {
    return callback(err);
  }
  this.request(Proto.CreateTable, params, callback);
};
/**
 * delete table
 * @param {string} tableName
 * @param {function(err)} callback
 */
Client.prototype.deleteTable = function (tableName, callback) {
  this.request(Proto.DeleteTable, {tableName: tableName}, callback);
};
/**
 * describe table
 * @param tableName {string}
 * @param callback {function(err)}
 */
Client.prototype.describeTable = function (tableName, callback) {
  this.request(Proto.DescribeTable, {tableName: tableName}, callback);
};
/**
 * list table
 * @param callback {function(err)}
 */
Client.prototype.listTables = function (callback) {
  this.request(Proto.ListTables, null, callback);
};
/**
 * insert one row into table
 * @param tableName {string}
 * @param row {{
 * key: {partitionKey, rowKey},
 * columns: {}
 * }}
 * @param callback
 */
Client.prototype.putRow = function (tableName, row, callback) {

  if (!tableName){
    throw 'invalid table name';
  }
  if (typeof row.key.partitionKey != 'number' && typeof row.key.partitionKey != 'string'){
    throw 'partition key type should be string or int64';
  }

  var param = {tableName: tableName};
  if (!row.hasOwnProperty('key'))
    callback('key of row has not exists!');
  param.key = {
    partitionKey: row.key.partitionKey,
    rowKey: row.key.rowKey
  };
  if (!row.hasOwnProperty('columns'))
    callback('no columns in row!');
  param.columns = [];
  for (var columnName in row.columns) {
    param.columns.push({
      columnName: columnName,
      columnValue: row.columns[columnName]
    });
  }
  this.request(Proto.PutRow, param, callback);
};
/**
 * get one row
 * @param tableName {string}
 * @param options
 * @param callback
 */
Client.prototype.getRow = function (tableName, options, callback) {
  if (!tableName){
    throw 'invalid table name';
  }
  if (!options.hasOwnProperty('key')){
    throw 'options must has key field';
  }
  var param = {
    tableName: tableName,
    key: options.key,
    strongConsistent: options.hasOwnProperty('strongConsistent')
      ? options.strongConsistent
      : false,
    columns: options.hasOwnProperty('columns')
      ? options.columns
      : null
  };
  this.request(Proto.GetRow, param, callback);
};
/**
 * delete one row
 * @param tableName
 * @param key
 * @param callback
 */
Client.prototype.deleteRow = function (tableName, key, callback) {
  if (!tableName){
    throw 'invalid table name';
  }

  this.request(Proto.DeleteRow, {
    tableName: tableName,
    key: key
  }, callback);
};
/**
 * scan table
 * @param tableName
 */
Client.prototype.scan = function (tableName) {
  var options = arguments.length === 3
    ? arguments[1]
    : {};
  var callback = arguments[arguments.length - 1];

  this.request(Proto.Scan, {
    tableName: tableName,
    strongConsistent: options.hasOwnProperty('strongConsistent')
      ? options.strongConsistent
      : false,
    limit: options.hasOwnProperty('limit')
      ? options.limit
      : null,
    startKey: options.hasOwnProperty('startKey')
      ? options.startKey
      : null,
    endKey: options.hasOwnProperty('endKey')
      ? options.endKey
      : null,
    columns: options.hasOwnProperty('columns')
      ? options.columns
      : null
  }, callback);
};
/**
 * batch write row
 * @param tableName {string}
 * @param rows
 * @param callback
 */
Client.prototype.batchWriteRow = function (tableName, rows, callback) {
  this.request(Proto.BatchWriteRow, {
    tableName: tableName,
    rows: rows
  }, callback);
};
/**
 * batch get row
 * @param tableName
 * @param keys
 */
Client.prototype.batchGetRow = function (tableName, keys) {
  var options = arguments.length === 4
    ? arguments[2]
    : {};
  var callback = arguments[arguments.length - 1];

  this.request(Proto.BatchGetRow, {
    tableName: tableName,
    keys: keys,
    strongConsistent: options.hasOwnProperty('strongConsistent')
      ? options.strongConsistent
      : false,
    columns: options.hasOwnProperty('columns')
      ? options.columns
      : null
  }, callback);
};

module.exports = Client;
