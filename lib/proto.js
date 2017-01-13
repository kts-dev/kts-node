'use strict';

const path = require('path');
const protoBuf = require('protobufjs');
const Long = protoBuf.Long;

var builder = protoBuf.newBuilder();
protoBuf.loadProtoFile(path.join(__dirname, '../proto/base.proto'));
protoBuf.loadProtoFile(path.join(__dirname, '../proto/filter.proto'), builder);
protoBuf.loadProtoFile(path.join(__dirname, '../proto/table.proto'), builder);
const Pb = builder.build('com.kingsoft.services.table.proto');
const utils = require('./base.js').utils;
const Encoder = require('./base.js').Encoder;
const Decoder = require('./base.js').Decoder;
const filter = require('./filter.js');

exports.checkResponse = function(pb) {
  if (pb.code !== Pb.Code.kOk) {
    throw utils.newError(pb.code, Decoder.code(pb.code), pb.msg);
  }
};

exports.CreateTable = {
  name: 'CreateTable',
  /*
   message CreateTableRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   required ColumnType partition_key_type = 3;
   optional ColumnType row_key_type = 4;
   required ProvisionedThroughput provisioned_throughput = 5;
   repeated GlobalSecondaryIndex global_secondary_indexes = 6;
   }
   */
  serialize: function(param) {
    return new Pb.CreateTableRequest({
      log_id: utils.genLogID(),
      table_name: param.tableName,
      partition_key_type: Encoder.columnType(param.schema.partitionKeyType),
      row_key_type: param.schema.rowKeyType
        ? Encoder.columnType(param.schema.rowKeyType)
        : null,
      provisioned_throughput: {
        read_capacity_units: param.provisionedThroughput.readCapacityUnits,
        write_capacity_units: param.provisionedThroughput.writeCapacityUnits
      }
    }).toBuffer();
  },
  /*
   message CreateTableResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional TableDescription table_description = 4;
   }
   */
  decode: Pb.CreateTableResponse.decode,
  unserialize: function(pb) {
    return Decoder.tableDescription(pb.table_description);
  },
  getInfo: utils.getInfo
};

exports.DeleteTable = {
  name: 'DeleteTable',
  /*
   message DeleteTableRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   }
   */
  serialize: function(param) {
    return new Pb.DeleteTableRequest({
      log_id: utils.genLogID(),
      table_name: param.tableName
    }).toBuffer();
  },
  /*
   message DeleteTableResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional TableDescription table_description = 4;
   }
   */
  decode: Pb.DeleteTableResponse.decode,
  unserialize: function(pb) {
    if (pb.table_description) {
      return Decoder.tableDescription(pb.table_description);
    } else {
      return {
        tableStatus: 'deleting'
      }
    }
  },
  getInfo: utils.getInfo
};

exports.DescribeTable = {
  name: 'DescribeTable',
  /*
   message DescribeTableRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   }
   */
  serialize: function(param) {
    return new Pb.DescribeTableRequest({
      log_id: utils.genLogID(),
      table_name: param.tableName
    }).toBuffer();
  },
  /*
   message DescribeTableResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional TableDescription table_description = 4;
   }
   */
  decode: Pb.DescribeTableResponse.decode,
  unserialize: function(pb) {
    return Decoder.tableDescription(pb.table_description);
  },
  getInfo: utils.getInfo
};

exports.ListTables = {
  name: 'ListTables',
  /*
   message ListTablesRequest {
   required int64 log_id = 1;
   }
   */
  serialize: function() {
    return new Pb.ListTablesRequest({
      log_id: utils.genLogID()
    }).toBuffer();
  },
  /*
   message ListTablesResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   repeated string table_names = 4;
   }
   */
  decode: Pb.ListTablesResponse.decode,
  unserialize: function(pb) {
    return pb.table_names;
  },
  getInfo: utils.getInfo
};

exports.PutRow = {
  name: 'PutRow',
  /*
   message PutRowRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   required Row row = 3;
   optional Condition condition = 4;
   optional int64 reserved = 5;
   }
   */
  serialize: function(param) {
    return new Pb.PutRowRequest({
      log_id: utils.genLogID(),
      table_name: param.tableName,
      row: {
        primary_key: Encoder.primaryKey(param.key),
        attribute_columns: Encoder.columns(param.columns)
      }
    }).toBuffer();
  },
  /*
   message PutRowResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional ConsumedCapacity consumed_capacity = 4;
   optional bool processed = 5;
   optional ConsumedCapacity index_consumed_capacity = 6;
   }
   */
  decode: Pb.PutRowResponse.decode,
  unserialize: function(pb) {
  },
  getInfo: utils.getInfo
};

exports.GetRow = {
  name: 'GetRow',
  /*
   message GetRowRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   required PrimaryKey primary_key = 3;
   repeated string column_names = 4;
   optional bool is_strong_consistent_read = 5 [default = false];
   optional Filter filter = 6;
   }
   */
  serialize: function(param) {
    return new Pb.GetRowRequest({
        log_id: utils.genLogID(),
        table_name: param.tableName,
        primary_key: Encoder.primaryKey(param.key),
        column_names: param.columns,
        is_strong_consistent_read: param.hasOwnProperty('strongConsistent')
          ? param.strongConsistent
          : false,
        filter: param.filter != null ? filter.parse(param.filter) : null
      }
    ).toBuffer();
  },
  /*
   message GetRowResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional Row row = 4;
   optional ConsumedCapacity consumed_capacity = 5;
   }
   */
  decode: Pb.GetRowResponse.decode,
  unserialize: function(pb) {
    return Decoder.row(pb.row);
  },
  getInfo: utils.getInfo
};

exports.DeleteRow = {
  name: 'DeleteRow',
  /*
   message DeleteRowRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   required PrimaryKey primary_key = 3;
   optional Condition condition = 4;
   }
   */
  serialize: function(param) {
    return new Pb.DeleteRowRequest({
      log_id: utils.genLogID(),
      table_name: param.tableName,
      primary_key: Encoder.primaryKey(param.key)
    }).toBuffer();
  },
  /*
   message DeleteRowResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional ConsumedCapacity consumed_capacity = 4;
   optional bool processed = 5;
   }
   */
  decode: Pb.DeleteRowResponse.decode,
  unserialize: function(pb) {
  },
  getInfo: utils.getInfo
};

exports.UpdateRow = {
  name: 'UpdateRow',
  /*
   message UpdateRowRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   required PrimaryKey primary_key = 3;
   repeated ColumnUpdate column_updates = 4;
   optional Condition condition = 5;
   }
   */
  serialize: function(param) {
    var columns = [];
    for (var name in param.columns) {
      if (utils.isDeleteAction(param.columns[name])) {
        columns.push({
          column_name: name,
          action: Pb.ActionType.kDelete
        });
      } else {
        columns.push({
          column_name: name,
          action: Pb.ActionType.kPut,
          column_value: Encoder.columnValue(param.columns[name])
        });
      }
    }
    return new Pb.UpdateRowRequest({
      log_id: utils.genLogID(),
      table_name: param.tableName,
      primary_key: Encoder.primaryKey(param.key),
      column_updates: columns
    }).toBuffer();
  },
  /*
   message UpdateRowResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional ConsumedCapacity consumed_capacity = 4;
   optional bool processed = 5;
   optional ConsumedCapacity index_consumed_capacity = 6;
   }
   */
  decode: Pb.UpdateRowResponse.decode,
  unserialize: function(pb) {
  },
  getInfo: utils.getInfo
};

exports.Scan = {
  name: 'Scan',
  /*
   message ScanRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   repeated string column_names = 3;
   optional bool is_strong_consistent_read = 4 [default = false];
   optional PrimaryKey inclusive_start_key = 5;
   optional PrimaryKey exclusive_end_key = 6;
   optional int32 limit = 7;
   optional Filter filter = 8;
   }
   */

  serialize: function(param) {
    return new Pb.ScanRequest({
      log_id: utils.genLogID(),
      table_name: param.tableName,
      column_names: param.columns,
      is_strong_consistent_read: param.strongConsistent,
      inclusive_start_key: param.startKey
        ? Encoder.primaryKey(param.startKey)
        : null,
      exclusive_end_key: param.endKey
        ? Encoder.primaryKey(param.endKey)
        : null,
      limit: param.limit,
      filter: param.filter != null ? filter.parse(param.filter) : null
    }).toBuffer();
  },
  /*
   message ScanResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   repeated Row rows = 4;
   optional PrimaryKey next_start_key = 5;
   optional ConsumedCapacity consumed_capacity = 6;
   }
   */
  decode: Pb.ScanResponse.decode,
  unserialize: function(pb) {
    var rows = [];
    pb.rows.forEach(function(row) {
      rows.push(Decoder.row(row));
    });
    return rows;
  },
  getInfo: function(pb) {
    var info = utils.getInfo(pb);
    if (pb.next_start_key) {
      info.nextStartKey = Decoder.primaryKey(pb.next_start_key);
    }
    return info;
  }
};

exports.BatchGetRow = {
  name: 'BatchGetRow',
  /*
   message BatchGetRowRequest {
   required int64 log_id = 1;
   repeated GetRowsRequest get_rows = 2;
   }
   message GetRowsRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   repeated PrimaryKey primary_keys = 3;
   repeated string column_names = 4;
   optional bool is_strong_consistent_read = 5 [default = false];
   optional Filter filter = 6;
   }
   */
  serialize: function(param) {
    var log_id = utils.genLogID();
    var keys = [];
    param.keys.forEach(function(key) {
      keys.push(Encoder.primaryKey(key));
    });
    return new Pb.BatchGetRowRequest({
      log_id: log_id,
      get_rows: {
        log_id: log_id,
        table_name: param.tableName,
        primary_keys: keys,
        column_names: param.hasOwnProperty('columns')
          ? param.columns
          : null,
        is_strong_consistent_read: param.hasOwnProperty('strongConsistent')
          ? param.strongConsistent
          : false,
        filter: param.filter != null ? filter.parse(param.filter) : null
      }
    }).toBuffer();
  },
  /*
   message BatchGetRowResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   repeated GetRowsResponse get_rows = 4;
   optional ConsumedCapacity consumed_capacity = 5;
   }
   message GetRowsResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional string table_name = 4;
   repeated Row rows = 5;
   optional GetRowsRequest unprocessed_rows = 6;
   optional ConsumedCapacity consumed_capacity = 7;
   }
   */
  decode: Pb.BatchGetRowResponse.decode,
  unserialize: function(pb) {
    var get_rows = pb.get_rows;
    if (get_rows.length !== 1) {
      throw new Error('batch get_rows length must be 1 but ' + get_rows.length)
    }
    var rows = [];
    get_rows[0].rows.forEach(function(row) {
      rows.push(Decoder.row(row));
    });
    return rows;
  },
  getInfo: utils.getInfo
};

exports.BatchWriteRow = {
  name: 'BatchWriteRow',
  /*
   message BatchWriteRowRequest {
   required int64 log_id = 1;
   repeated WriteRowsRequest write_rows = 2;
   }
   message WriteRowsRequest {
   required int64 log_id = 1;
   required string table_name = 2;
   repeated PutRowRequest put_rows = 3;
   repeated UpdateRowRequest update_rows = 4;
   repeated DeleteRowRequest delete_rows = 5;
   }
   */
  serialize: function(param) {
    var log_id = utils.genLogID();
    var put_rows = [];
    var update_rows = [];
    var delete_rows = [];

    param.rows.forEach(function(row) {
      var key = Encoder.primaryKey(row.key);
      var columns = [];

      if (row.action === 'DELETE') {
        if (row.hasOwnProperty('columns') && row.columns) {  // delete columns
          row.columns.forEach(function(col) {
            columns.push({
              column_name: col,
              action: Pb.ActionType.kDelete
            });
          });
          update_rows.push({
            log_id: log_id,
            table_name: param.tableName,
            primary_key: key,
            column_updates: columns
          });

        } else {  // delete row
          delete_rows.push({
            log_id: log_id,
            table_name: param.tableName,
            primary_key: key
          });
        }

      } else if (row.action === 'PUT' || !row.hasOwnProperty('action')) {  // put row
        for (var name in row.columns) {
          if (row.columns.hasOwnProperty(name)) {
            columns.push({
              columnName: name,
              columnValue: row.columns[name]
            });
          }
        }
        put_rows.push({
          log_id: log_id,
          table_name: param.tableName,
          row: {
            primary_key: key,
            attribute_columns: Encoder.columns(columns)
          }
        });
      } else {
        throw new Error('illegal action in batchWriteRow:' + row.action);
      }
    });

    return new Pb.BatchWriteRowRequest({
      log_id: log_id,
      write_rows: {
        log_id: log_id,
        table_name: param.tableName,
        put_rows: put_rows,
        update_rows: update_rows,
        delete_rows: delete_rows
      }
    }).toBuffer();
  },
  /*
   message BatchWriteRowResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   repeated WriteRowsResponse write_rows = 4;
   optional ConsumedCapacity consumed_capacity = 5;
   }
   message WriteRowsResponse {
   required int64 log_id = 1;
   required int32 code = 2;
   required string msg = 3;
   optional string table_name = 4;
   optional WriteRowsRequest unprocessed_rows = 5;
   optional ConsumedCapacity consumed_capacity = 6;
   optional bool processed = 7;
   optional ConsumedCapacity index_consumed_capacity = 8;
   }
   */
  decode: Pb.BatchWriteRowResponse.decode,
  unserialize: function(pb) {
    var write_rows = pb.write_rows;
    if (write_rows.length !== 1) {
      throw new Error('batch write_rows length must be 1 but ' + write_rows.length);
    }
  },
  getInfo: utils.getInfo
};
