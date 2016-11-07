'use strict';

const http = require('http');
const url = require('url');
const BufferHelper = require('bufferhelper');

const proto = require('./proto.js');
const restriction = require('./restriction.js');
const Signer = require('./signer.js');

function setError(code, name, err) {
  err.code = code;
  err.name = name;
  return err;
}

function noticeError(err, callback) {
  if (callback)
    return process.nextTick(function() {
      callback(err);
    });
  throw err;
}

function Client(options) {
  this.endpoint = options.endpoint;
  this.accessKey = options.accessKey;
  this.secretKey = options.secretKey;
  this.xVersion = options.xVersion || '2016-03-01';

  var address = url.parse(options.endpoint);
  this.hostname = address.hostname;
  this.port = address.port;

  this.signer = new Signer({
    accessKey: this.accessKey,
    secretKey: this.secretKey
  });
  this.agent = new http.Agent({KeepAlive: true});
}

Client.prototype.request = function (action, param, callback) {
  try {
    var buff = action.serialize(param);
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
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
      'X-Kws-Date': new Date().toISOString(),
      'Content-Type': 'application/binary'
    },
    agent: this.agent
  };
  http_options.headers['Authorization'] = this.signer.sign(http_options.method, http_options.headers);

  var req = http.request(http_options, function (res) {
    res.on("error", function (err) {
      return noticeError(setError(-2, 'requestFailed', err), callback);
    });

    var body = new BufferHelper();
    res.on("data", function (data) {
      body.concat(data);
    });

    res.on("end", function () {
      try {
        var pb = action.decode(body.toBuffer());
      } catch (err) {
        err.body = body.toString();
        return noticeError(setError(-3, 'apiError', err), callback);
      }
      try {
        proto.checkResponse(pb);
        var result = action.unserialize(pb);
        var info = action.getInfo(pb);
      } catch (err) {
        return noticeError(err, callback);
      }
      if (callback)
        return callback(null, result, info);
    });
  });

  req.on("error", function (err) {
    return noticeError(setError(-2, 'requestError', err), callback);
  });

  req.write(buff);
  req.end();
};

Client.prototype.createTable = function (tableName) {
  var options = arguments.length >= 3
    ? arguments[1]
    : {};
  var callback = arguments.length >= 2
    ? arguments[arguments.length - 1]
    : null;

  var params = {tableName: tableName};
  try {
    params.schema = options.schema || {
        partitionKeyType: "STRING",
        rowKeyType: null
      };
    params.provisionedThroughput = options.provisionedThroughput || {
        readCapacityUnits: 10,
        writeCapacityUnits: 10
      };

    restriction.tableName(params.tableName);
    restriction.schema(params.schema);
    restriction.provisionedThroughput(params.provisionedThroughput);
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.CreateTable, params, callback);
};

Client.prototype.deleteTable = function (tableName, callback) {
  try {
    restriction.tableName(tableName);
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.DeleteTable, {tableName: tableName}, callback);
};

Client.prototype.describeTable = function (tableName, callback) {
  try {
    restriction.tableName(tableName);
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.DescribeTable, {tableName: tableName}, callback);
};

Client.prototype.listTables = function (callback) {
  this.request(proto.ListTables, null, callback);
};

Client.prototype.putRow = function (tableName, row, callback) {
  try {
    restriction.tableName(tableName);
    restriction.row(row);
    var param = {
      tableName: tableName,
      key: row.key,
      columns: []
    };
    for (var name in row.columns) {
      if (row.columns.hasOwnProperty(name)) {
        param.columns.push({
          columnName: name,
          columnValue: row.columns[name]
        });
      }
    }
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.PutRow, param, callback);
};

Client.prototype.getRow = function (tableName, options, callback) {
  try {
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
    restriction.tableName(param.tableName);
    restriction.primaryKey(param.key);
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.GetRow, param, callback);
};

Client.prototype.deleteRow = function (tableName, key, callback) {
  try {
    restriction.tableName(tableName);
    restriction.primaryKey(key);
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.DeleteRow, {
    tableName: tableName,
    key: key
  }, callback);
};

Client.prototype.scan = function (tableName) {
  var options = arguments.length >= 3
    ? arguments[1]
    : {};
  var callback = arguments.length >= 2
    ? arguments[arguments.length - 1]
    : null;
  try {
    restriction.tableName(tableName);
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.Scan, {
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

Client.prototype.batchWriteRow = function (tableName, rows, callback) {
  try {
    restriction.tableName(tableName);
    for (var i = 0; i < rows.length; ++i) {
      restriction.primaryKey(rows[i].key);
    }
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.BatchWriteRow, {
    tableName: tableName,
    rows: rows
  }, callback);
};

Client.prototype.batchGetRow = function (tableName, keys) {
  var options = arguments.length >= 4
    ? arguments[2]
    : {};
  var callback = arguments.length >= 3
    ? arguments[arguments.length - 1]
    : null;
  try {
    restriction.tableName(tableName);
    for (var i = 0; i < keys.length; ++i) {
      restriction.primaryKey(keys[i]);
    }
  } catch (err) {
    return noticeError(setError(-1, 'parseParamError', err), callback);
  }
  this.request(proto.BatchGetRow, {
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
