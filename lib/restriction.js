'use strict';

exports.tableName = function (name) {
  if (typeof name !== 'string') {
    throw new Error('illegal type of tableName:' + name);
  }
};

exports.schema = function (schema) {
  if (typeof schema !== 'object') {
    throw new Error('illegal type of object:' + schema);
  }
  if (!schema.partitionKeyType) {
    throw new Error('miss partitionKeyType in schema');
  }
  if (schema.partitionKeyType !== 'STRING' && schema.partitionKeyType !== 'INT64') {
    throw new Error('illegal type name of partitionKeyType:' + schema.partitionKeyType);
  }
  if (schema.rowKeyType) {
    if (schema.rowKeyType !== 'STRING' && schema.rowKeyType !== 'INT64') {
      throw new Error('illegal type name of rowKeyType:' + schema.rowKeyType);
    }
  }
};

exports.primaryKey = function (key) {
  if (typeof key !== 'object') {
    throw new Error('illegal type of primaryKey:' + key);
  }
  if (key instanceof Array) {
    // as [partitionKey, rowKey]
  } else {
    if (!key.hasOwnProperty('partitionKey')) {
      throw new Error('miss partitionKey in primary key');
    }
  }
};

exports.provisionedThroughput = function (pt) {
  if (typeof pt !== 'object') {
    throw new Error('illegal type of provisionedThoughput:' + pt);
  }
  if (!pt.hasOwnProperty('readCapacityUnits')) {
    throw new Error('miss readCapacityUnits in provisionedThroughput');
  }
  if (!pt.hasOwnProperty('writeCapacityUnits')) {
    throw new Error('miss writeCapacityUnits in provisionedThroughput');
  }
};

exports.columns = function(columns) {
  if (typeof columns !== 'object') {
    throw new Error('illegal type of columns:' + columns);
  }
};

exports.row = function(row) {
  if (!row.key) {
    throw new Error('miss key in row');
  }
  this.primaryKey(row.key);
  if (!row.columns) {
    throw new Error('miss columns in row');
  }
  this.columns(row.columns);
};