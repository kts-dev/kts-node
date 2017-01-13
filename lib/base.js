/**
 * Created by baibin on 2017/1/5.
 */

'use strict';

const path = require('path');
const protoBuf = require('protobufjs');
const Long = protoBuf.Long;

var builder = protoBuf.newBuilder();
protoBuf.loadProtoFile(path.join(__dirname, '../proto/base.proto'));
protoBuf.loadProtoFile(path.join(__dirname, '../proto/filter.proto'), builder);
protoBuf.loadProtoFile(path.join(__dirname, '../proto/table.proto'), builder);
const Pb = builder.build('com.kingsoft.services.table.proto');

const utils = {
  genLogID: function(str) {
    if (str) {
      return Long.fromString(str);
    }
    return new Long(Math.random() * Number.MAX_SAFE_INTEGER, Math.random() * Number.MAX_SAFE_INTEGER, true);
  },

  isDeleteAction: function(value) {
    return value.action === 'DELETE';
  },

  int64ToUint: function(v) {
    v.unsigned = true;
    return v.toInt();
  },

  getInfo: function(res) {
    return {
      logId: Decoder.logId(res.log_id),
      consumedCapacity: res.consumed_capacity
        ? utils.int64ToUint(res.consumed_capacity.capacity_units)
        : 0
    }
  },

  newError: function(code, name, msg) {
    var err = new Error(msg);
    return this.setError(code, name, err);
  },

  setError: function(code, name, err) {
    err.code = code;
    err.name = name;
    return err;
  }
};

const Encoder = {
  columnType: function(type) {
    const map = {
      'STRING': Pb.ColumnType.kString,
      'INT64': Pb.ColumnType.kInt64,
      'BOOLEAN': Pb.ColumnType.kBoolean,
      'DOUBLE': Pb.ColumnType.kDouble
    };
    if (map.hasOwnProperty(type)) {
      return map[type];
    }
    throw utils.newError(-1, 'parseParamError', 'Type name:' + type + ' not supported');
  },

  columnValue: function(value) {
    var type = typeof value;
    if (type === 'number') {
      if (value % 1 === 0 && value < Number.MAX_VALUE) {
        value = Long.fromNumber(value);
        type = 'int64';
      } else {
        type = 'double';
      }
    }

    const map = {
      'int64': {t: Pb.ColumnType.kInt64, f: 'int64_value'},
      'string': {t: Pb.ColumnType.kString, f: 'string_value'},
      'boolean': {t: Pb.ColumnType.kBoolean, f: 'bool_value'},
      'double': {t: Pb.ColumnType.kDouble, f: 'double_value'}
    };
    if (map.hasOwnProperty(type)) {
      var cv = {column_type: map[type].t};
      cv[map[type].f] = value;
      return cv;
    }
    throw utils.newError(-1, 'parseParamError', 'not supported type: ' + type);
  },

  columns: function(values) {
    var cols = [];
    values.forEach(function(column) {
      if (column.columnValue !== undefined && column.columnValue !== null) {
        cols.push({
          column_name: column.columnName,
          column_value: Encoder.columnValue(column.columnValue)
        });
      }
    });
    return cols;
  },

  primaryKey: function(key) {
    return {
      partition_key: Encoder.columnValue(key.partitionKey),
      row_key: key.hasOwnProperty('rowKey') && key.rowKey !== undefined && key.rowKey !== null
        ? Encoder.columnValue(key.rowKey)
        : null
    }
  }
};

const Decoder = {
  code: function(code) {
    switch (code) {
      case Pb.Code.kOk:
        return 'ok';
      case Pb.Code.kAccessDeniedException:
        return 'accessDenied';
      case Pb.Code.kLimitExceededException:
        return 'limitExceeded';
      case Pb.Code.kProvisionedThroughputExceededException:
        return 'provisionedThroughputExceeded';
      case Pb.Code.kResourceInUseException:
        return 'resourceInUse';
      case Pb.Code.kResourceNotFoundException:
        return 'resourceNotFound';
      case Pb.Code.kThrottlingException:
        return 'throttlingException';
      case Pb.Code.kValidationException:
        return 'validation';
      case Pb.Code.kInternalServerError:
        return 'internalServerError';
      case Pb.Code.kServiceUnavailableException:
        return 'serviceUnavailable';
      case Pb.Code.kConditionCheckException:
        return 'conditionCheckFailed';
      default:
        return 'internalError:' + code;
    }
  },
  tableStatus: function(code) {
    switch (code) {
      case Pb.TableStatus.kCreatingTable:
        return 'creating';
      case Pb.TableStatus.kUpdatingTable:
        return 'updating';
      case Pb.TableStatus.kDeletingTable:
        return 'deleting';
      case Pb.TableStatus.kActiveTable:
        return 'active';
      case Pb.TableStatus.kInActiveTable:
        return 'inactive';
      default:
        return 'UnKnown:' + code.toString();
    }
  },

  columnType: function(type) {
    switch (type) {
      case Pb.ColumnType.kString:
        return 'STRING';
      case type === Pb.ColumnType.kInt64:
        return 'INT64';
      case type === Pb.ColumnType.kDouble:
        return 'DOUBLE';
      case Pb.ColumnType.kBoolean:
        return 'BOOLEAN';
      default:
        return 'UnKnown:' + type.toString();
    }
  },

  logId: function(logId) {
    logId.unsigned = true;
    return logId.toString();
  },

  columnValue: function(value) {
    switch (value.column_type) {
      case Pb.ColumnType.kInt64:
        return value.int64_value.toNumber();
      case Pb.ColumnType.kString:
        return value.string_value;
      case Pb.ColumnType.kDouble:
        return value.double_value;
      case Pb.ColumnType.kBoolean:
        return value.bool_value;
      default:
        throw new Error('UnKnownValue:' + value);
    }
  },

  primaryKey: function primaryKey(key) {
    var primaryKey = {
      partitionKey: Decoder.columnValue(key.partition_key)
    };
    if (key.row_key) {
      primaryKey.rowKey = Decoder.columnValue(key.row_key);
    }
    return primaryKey;
  },

  columns: function columns(cols) {
    var columns = {};
    cols.forEach(function(col) {
      columns[col.column_name] = Decoder.columnValue(col.column_value);
    });
    return columns;
  },

  row: function row(row) {
    return {
      key: Decoder.primaryKey(row.primary_key),
      columns: Decoder.columns(row.attribute_columns)
    }
  },

  tableDescription: function tableDescription(res) {
    var schema = {
      partitionKeyType: Decoder.columnType(res.partition_key_type)
    };
    if (res.row_key_type !== null) {
      schema.rowKeyType = Decoder.columnType(res.row_key_type);
    }
    return {  // todo add more information
      tableId: res.table_id,
      tableName: res.table_name,
      tableStatus: Decoder.tableStatus(res.table_status),
      schema: schema,
      creationDateTime: res.creation_date_time,
      provisionedThroughput: {
        readCapacityUnits: utils.int64ToUint(res.provisioned_throughput.read_capacity_units),
        writeCapacityUnits: utils.int64ToUint(res.provisioned_throughput.write_capacity_units)
      }
    }
  }
};

exports.utils = utils;
exports.Encoder = Encoder;
exports.Decoder = Decoder;