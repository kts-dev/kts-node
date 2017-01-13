var should = require('should');
const Kts = require('../lib/kts.js');
const config = require('./config.js');
var EventProxy = require('eventproxy');

describe('kts_client_test.js', function () {
  var client = new Kts({
    endpoint: config.endpoint,
    accessKey: config.accessKey,
    secretKey: config.secretKey
  });

  var describeUntilStatus = function (tableName, status, callback) {
    describeTableFunc(tableName, function (err, res) {
      if (err){
        console.error(err);
        throw err;
      }
      if (res){
        if (res.tableStatus == status) {
          callback(err, res);
        } else {
            setTimeout(function(tableName, status, callback){
              describeUntilStatus(tableName, status, callback)
          }, 20, tableName, status, callback);
        }
      }
    });
  };

  var describeUntilNotFound = function (tableName, callback) {
    describeTableFunc(tableName, function (err, result) {
      if (err) {
        if (err.name == 'resourceNotFound') {
          callback(err, result);
        } else {
          throw err;
        }
      } else {
        setTimeout(function(tableName, callback){
          describeUntilNotFound(tableName, callback);
        }, 20, tableName, callback);
      }
    });
  };

  var createUntilDone = function (tableName, partitionKeyType, rowKeyType, readCU, writeCU, callback) {
    createTableFunc(tableName, partitionKeyType, rowKeyType, readCU, writeCU, function (err) {
      if (err && err.name == 'resourceInUse') {
        console.error("resourceInUse");
        return;
      }
      describeUntilStatus(tableName, 'active', callback);
    });
  };

  var deleteUntilDone = function (tableName, callback) {
    deleteTableFunc(tableName, function (err, result) {
      if (err) {
        if (err.name == "resourceNotFound"){
          callback(err, result)
        } else {
          throw err;
        }
      } else {
        describeUntilNotFound(tableName, function (err, result) {
          callback(err, result);
        })
      }
    })
  };

  var checkTableValid = function (res, readCU, writeCU) {
    should.exists(res);
    should.equal(res.tableStatus, 'active');
    should.equal(res.provisionedThroughput.readCapacityUnits, readCU);
    should.equal(res.provisionedThroughput.writeCapacityUnits, writeCU);
  };

  var checkObjectEquality = function (obj1, obj2) {
    var keys1 = Object.keys(obj1);
    var keys2 = Object.keys(obj2);

    for (var key in obj1){
      if (obj1[key] != obj2[key]){
        return false;
      }
    }

    for (var key in obj2){
      if (obj2[key] != obj1[key]){
        return false;
      }
    }
    return true;
  };

  var checkRowValid = function (res, partitionKey, rowKey, columns) {
    should.equal(res.key.partitionKey, partitionKey);
    should.equal(res.key.rowKey, rowKey);
    should.equal(checkObjectEquality(res.columns, columns), true);
  };

  var shouldRowNotExists = function (res) {
    should.equal(Object.keys(res.columns).length, 0);
  };

  var deleteTableFunc = function (tableName, callback) {
    client.deleteTable(tableName, callback)
  };

  var listTableFunc = function (callback) {
    client.listTables(callback);
  };

  var createTableFunc = function (tableName, partitionKeyType, rowKeyType, readCU, writeCU, callback) {
    client.createTable(tableName, {
        schema: {
          partitionKeyType: partitionKeyType,
          rowKeyType: rowKeyType
        },
        provisionedThroughput: {
          readCapacityUnits: readCU,
          writeCapacityUnits: writeCU
        }
      },
      callback);
  };

  var describeTableFunc = function (tableName, callback) {
    client.describeTable(tableName, callback);
  };


  var putRowFunc = function (tableName, partitionKey, rowKey, columns, callback) {
    client.putRow(tableName, {
      key: {
        partitionKey: partitionKey,
        rowKey: rowKey
      },
      columns: columns
    }, callback);
  };

  var getRowFunc = function (tableName, partitionKey, rowKey, consistent, columns, callback) {
    client.getRow(tableName, {
      key: {
        partitionKey: partitionKey,
        rowKey: rowKey
      },
      strongConsistent: consistent,  // optional, default is false
      columns: columns
    }, callback);
  };

  var deleteRowFunc = function (tableName, partitionKey, rowKey, callback) {
    client.deleteRow(tableName, {
      partitionKey: partitionKey,
      rowKey: rowKey
    }, callback);
  };

  var batchGetRowFunc = function (tableName, keys) {
    var options = arguments.length === 4
      ? arguments[2]
      : {};
    var callback = arguments[arguments.length - 1];

    client.batchGetRow(tableName, keys, options, callback);
  };

  var batchWriteRowFunc = function (tableName, rows, callback) {
    client.batchWriteRow(tableName, rows, callback);
  };

  var scanFunc = function (tableName, startKey, endKey, callback){
    var options = {
      startKey: startKey,
      endKey: endKey,
      strongConsistent: true
    }
    client.scan(tableName, options,callback);
  };

  var ScanAllFunc = function (tableName, callback){
    var scanCallback = function(err, data, info){
      console.log(info);
      callback(err, data, info);
      if (info.hasOwnProperty('nextStartKey')){
        scanFunc(tableName, info.nextStartKey, null, scanCallback);
      }
    };
    scanFunc(tableName, null, null, scanCallback);
  };

  before(function (done) {
    done();
    // var ep = new EventProxy();
    // this.timeout(5000)
    // listTableFunc(function (err, tables) {
    //   if (err) {
    //     console.log(err)
    //   }
    //
    //   if (tables) {
    //     var table_count_now = tables.length;
    //     ep.after('delete_all_table_done', table_count_now, function (list) {
    //       console.log('before, delete all table done\n')
    //       done();
    //     });
    //
    //     tables.forEach(function (tableName) {
    //       deleteUntilDone(tableName, function (err, data) {
    //         ep.emit('delete_all_table_done', 'done');
    //       });
    //     });
    //   }
    // });
  });

  beforeEach(function (done) {
    var ep = new EventProxy();

    ep.all('table_client_test_deleted', function () {
      createUntilDone('table_client_test', 'STRING', 'STRING', 5000, 5000, function (err, res) {
        setTimeout(function() {
          console.log("beforeEach create table table_client_test done\n");
          done();
        }, 1000);
      });
    });

    deleteUntilDone('table_client_test', function (err, res) {
      setTimeout(function() {
        console.log('beforeEach delete table_client_test done\n');
        ep.emit('table_client_test_deleted');
      }, 1000);
    });
  });

  describe('createTable', function () {
    var ep = EventProxy();
    ep.all('create_partition_key_string', 'create_partition_key_int', 'create_partition_key_bool', 'create_partition_row_key',
      'create_without_partition_key', 'create_throughput_neg', 'create_throughput_zero', 'create_throughput_empty',
      'create_throughput_big',
      function () {
        console.log('createTable done\n');
        deleteUntilDone('table_client_test_string_null', function () {
        });
        deleteUntilDone('table_client_test_int_null', function () {
        });
        deleteUntilDone('table_client_test_bool_null', function () {
        });
        deleteUntilDone('table_client_test_string_string', function () {
        });
        deleteUntilDone('table_client_test_null_string', function () {
        });
        deleteUntilDone('table_client_test_string_null_throughput_neg', function () {
        });
        deleteUntilDone('table_client_test_string_null_throughput_zero', function () {
        });
        deleteUntilDone('table_client_test_string_null_throughput_empty', function () {
        });
        deleteUntilDone('table_client_test_string_null_throughput_big', function () {
        });
      });
    it('create_partition_key_string should success', function (done) {
      ep.all('table_client_test_string_null_deleted', function () {
        createUntilDone('table_client_test_string_null', 'STRING', null, 100, 100, function (err, res) {
          should.not.exists(err);
          checkTableValid(res, 100, 100);
          ep.emit('create_partition_key_string');
          done();
        });
      });
      deleteUntilDone('table_client_test_string_null', function (err, res) {
        ep.emit('table_client_test_string_null_deleted');
      });
    });

    it('create_partition_key_bool should fail', function (done) {
      ep.all('table_client_test_bool_null_deleted', function () {
        createTableFunc('table_client_test_bool_null', 'BOOLEAN', null, 10, 10, function (err, res) {
          should.exists(err);
          ep.emit('create_partition_key_bool');
          done();
        });
      });
      deleteUntilDone('table_client_test_bool_null', function (err, res) {
        ep.emit('table_client_test_bool_null_deleted');
      });
    });

    it('create_partition_key_int should success', function (done) {
      ep.all('table_client_test_int_null_deleted', function () {
        createUntilDone('table_client_test_int_null', 'INT64', null, 50, 20, function (err, res) {
          should.not.exists(err);
          checkTableValid(res, 50, 20);
          ep.emit('create_partition_key_int');
          done();
        });
      });
      deleteUntilDone('table_client_test_int_null', function (err, res) {
        ep.emit('table_client_test_int_null_deleted');
      });
    });

    it('create_partition_row_key should success', function (done) {
      ep.all('table_client_test_string_string_deleted', function () {
        createUntilDone('table_client_test_string_string', 'STRING', 'STRING', 50, 20, function (err, res) {
          should.not.exists(err);
          checkTableValid(res, 50, 20);
          ep.emit('create_partition_row_key');
          done();
        });
      });
      deleteUntilDone('table_client_test_string_string', function (err, res) {
        ep.emit('table_client_test_string_string_deleted');
      });
    });

    it('create_without_partition_key should fail', function (done) {
      ep.all('table_client_test_null_string_deleted', function () {
        createTableFunc('table_client_test_null_string', 'STRING', 'STRING', 50, 20, function (err, res) {
          should.not.exists(err);
          ep.emit('create_without_partition_key');
          done();
        });
      });
      deleteUntilDone('table_client_test_null_string', function (err, res) {
        ep.emit('table_client_test_null_string_deleted');
      });
    });

    it('create_throughput_neg should fail', function (done) {
      ep.all('table_client_test_string_null_throughput_neg_deleted', function () {
        createTableFunc('table_client_test_string_null_throughput_neg', 'STRING', null, -1, 20, function (err, res) {
          console.log(err + '\n');
          should.exists(err);
          ep.emit('create_throughput_neg');
          done();
        });
      });
      deleteUntilDone('table_client_test_string_null_throughput_neg', function (err, res) {
        ep.emit('table_client_test_string_null_throughput_neg_deleted');
      });
    });

    it('create_throughput_zero should fail', function (done) {
      ep.all('table_client_test_string_null_throughput_zero_deleted', function () {
        createTableFunc('table_client_test_string_null_throughput_zero', 'STRING', null, 50, 0, function (err, res) {
          should.exists(err);
          console.log(err + '\n');
          ep.emit('create_throughput_zero');
          done();
        });
      });
      deleteUntilDone('table_client_test_string_null_throughput_zero', function (err, res) {
        ep.emit('table_client_test_string_null_throughput_zero_deleted');
      });
    });

    it('create_throughput_empty should fail', function (done) {
      ep.all('table_client_test_string_null_throughput_empty_deleted', function () {
        createTableFunc('table_client_test_string_null_throughput_empty', 'STRING', null, null, null, function (err, res) {
          should.exists(err);
          console.log(err + '\n');
          ep.emit('create_throughput_empty');
          done();
        });
      });
      deleteUntilDone('table_client_test_string_null_throughput_empty', function (err, res) {
        ep.emit('table_client_test_string_null_throughput_empty_deleted');
      });
    });

    it('create_throughput_big should fail', function (done) {
      ep.all('table_client_test_string_null_throughput_big_deleted', function () {
        createTableFunc('table_client_test_string_null_throughput_big', 'STRING', null, 100000000, 0, function (err, res) {
          should.exists(err);
          console.log(err + '\n');
          ep.emit('create_throughput_big');
          done();
        });
      });
      deleteUntilDone('table_client_test_string_null_throughput_big', function (err, res) {
        ep.emit('table_client_test_string_null_throughput_big_deleted');
      });
    });
  });


  describe('describeTable', function () {
    var ep = EventProxy();
    ep.all('describe_before_create', 'describe_after_create',
      function () {
        console.log('describeTable done\n');
        deleteUntilDone('table_client_test_describe_before_create', function () {
        });
        deleteUntilDone('table_client_test_describe_after_create', function () {
        });
      });

    it('describe_before_create should fail', function (done) {
      describeTableFunc('table_client_test_describe_before_create', function (err, res) {
        should.exists(err);
        ep.emit('describe_before_create');
        done();
      });
    });

    it('describe_after_create should success', function (done) {
      ep.all('table_client_test_describe_after_create_deleted', function () {
        createUntilDone('table_client_test_describe_after_create', 'STRING', null, 100, 100, function (err, res) {
          describeTableFunc('table_client_test_describe_after_create', function (err, res) {
            should.not.exists(err);
            checkTableValid(res, 100, 100);
            ep.emit('describe_after_create');
            done()
          })
        });
      });
      deleteUntilDone('table_client_test_describe_after_create', function (err, res) {
        ep.emit('table_client_test_describe_after_create_deleted');
      });
    });
  });

  // describe('listTable', function () {
  //   var ep = EventProxy();
  //
  //   it('listTable should success', function (done) {
  //     this.timeout(5000);
  //     listTableFunc(function (err, res) {
  //       if (err) {
  //         console.log(err)
  //       }
  //       if (res) {
  //         var table_count_now = res.length;
  //         ep.after('delete_all_table_done', table_count_now, function (list) {
  //           ep.emit('list_table_create_begin')
  //         });
  //
  //         res.forEach(function(tableName){
  //           deleteUntilDone(tableName, function (err, data) {
  //             ep.emit('delete_all_table_done', 'done');
  //           });
  //         });
  //       }
  //     });
  //
  //     ep.all('list_table_create_begin', function () {
  //       for (var i = 0; i < 25; i++) {
  //         createUntilDone('list_table_create_' + i.toString(), 'STRING', 'STRING', 100, 100, function (err, res) {
  //           ep.emit('list_table_create_end');
  //         })
  //       }
  //     });
  //
  //     ep.after('list_table_create_end', 25, function (list) {
  //       listTableFunc(function (err, data) {
  //         should.not.exists(err)
  //         should.equal(data.length, 25)
  //         done();
  //       })
  //     })
  //   });
  // });

  describe('deleteTable', function () {

    it('delete table should success', function (done) {
      deleteTableFunc('table_client_test', function (err, res) {
        should.not.exists(err);
        done();
      });
    });

    it('delete again should fail', function (done) {
      deleteTableFunc('table_client_test', function (err, res) {
        should.not.exists(err);
        describeUntilNotFound('table_client_test', function () {
          deleteTableFunc('table_client_test', function (err, res) {
            should.exists(err);
            done();
          });
        });
      });
    });
  });

  describe("singleRow", function () {
    var ep = new EventProxy();

    var columns = {
      'name': 'Jack',  // string
      'age': 30,  // int
      'weight': 62.3,  // double, not recommend
      'is_vip': true  // bool
    };

    it('no partition key put should fail', function (done) {
      try {
        putRowFunc('table_client_test', null, 'row_key_1', columns, function (err, res) {
          should.exists(err);
          done();
        });
      } catch (err) {
        done();
      }
    });

    it('no row key put should fail', function (done) {
      try {
        putRowFunc('table_client_test', 'test_key_1', null, columns, function (err, res) {
          should.exists(err);
          done();
        });
      } catch (err) {
        done();
      }
    });

    // it('big int partition key put should fail', function (done) {
    //   createTableFunc('table_client_test_int64_key', 'INT64', null, 100, 100, function () {
    //     try {
    //       putRowFunc('table_client_test_int64_key', 1000000000000000000000000000000000000000000000000000000000000000000000000000000, null, columns, function (err, res) {
    //         should.not.exists(err);
    //         getRowFunc('table_client_test_int64_key', 1000000000000000000000000000000000000000000000000000000000000000000000000000000, null, true, null, function (err, data) {
    //           console.log(data);
    //           done();
    //         });
    //       });
    //     } catch (err) {
    //       console.log(err);
    //       done();
    //     }
    //   })
    // });

    // it('large row key put should fail', function (done) {
    //   this.timeout(100000);
    //   try {
    //     require('fs').readFile(require('path').join(__dirname, 'Wildlife.wmv'), function read(err, data) {
    //       if (err) {
    //         throw err;
    //       }
    //       var columns = {
    //         'largeColumn': data.toString()
    //       };
    //       putRowFunc('table_client_test', 'test_key_1', 'row_key_large_data', columns, function (err, res) {
    //         should.exists(err);
    //         done();
    //       });
    //     });
    //   } catch (err) {
    //     done();
    //   }
    // });


    it('get row before put should fail', function (done) {
      getRowFunc('table_client_test', 'test_key_1', 'row_key_1', false, null, function (err, data) {
        shouldRowNotExists(data);
        done();
      })
    });

    it('normal put-get should success', function (done) {

      putRowFunc('table_client_test', 'test_key_1', 'row_key_2', columns, function (err, res) {
        should.not.exists(err);
        getRowFunc('table_client_test', 'test_key_1', 'row_key_2', true, ['age'], function (err, data) {
          should.not.exists(err);
          should.equal(Object.keys(data.columns).length, 1);
          checkRowValid(data, 'test_key_1', 'row_key_2', {'age': 30});
          done();
        })
      });
    });

    it('get row after delete should fail', function (done) {
      deleteRowFunc('table_client_test', 'test_key_1', 'row_key_2', function (err, data) {
        should.not.exists(err);
        getRowFunc('table_client_test', 'test_key_1', 'row_key_2', true, null, function (err, data) {
          shouldRowNotExists(data);
          done();
        })
      });
    });

    it('write-read-update-read should success', function (done) {
      putRowFunc('table_client_test', 'test_key_update', 'row_key_update', columns, function (err, res) {
        should.not.exists(err);
        getRowFunc('table_client_test', 'test_key_update', 'row_key_update', true, ['age'], function (err, data) {
          should.not.exists(err);
          checkRowValid(data, 'test_key_update', 'row_key_update', {'age': 30});
          var updateColumns = columns;
          updateColumns['age'] = 20;
          putRowFunc('table_client_test', 'test_key_update', 'row_key_update', updateColumns, function (err, data) {
            should.not.exists(err);
            getRowFunc('table_client_test', 'test_key_update', 'row_key_update', true, ['age'], function (err, data) {
              should.not.exists(err);
              checkRowValid(data, 'test_key_update', 'row_key_update', {'age': 20});
              done();
            });
          });
        })
      });
    });

    it('delete non-exist row should success', function (done) {
      deleteRowFunc('table_client_test', 'test_key_non_exist_delete', 'row_key_non_exist', function (err, res) {
        should.not.exists(err);
        done();
      });
    });

    it('delete row multi times should success', function (done) {
      putRowFunc('table_client_test', 'test_key_delete_multi_times', 'row_key_delete', columns, function (err, data) {
        should.not.exists(err);
        deleteRowFunc('table_client_test', 'test_key_delete_multi_times', 'row_key_delete', function (err, data) {
          should.not.exists(err);
          deleteRowFunc('table_client_test', 'test_key_delete_multi_times', 'row_key_delete', function (err, data) {
            should.not.exists(err);
            done();
          })
        });
      })
    });
  });


  describe('batchRow', function () {
    var columns = {
      'column_str': 'Jack',  // string
      'column_int': 30,  // int
      'column_double': 62.3,  // double, not recommend
      'column_bool': true  // bool
    };

    it('batch get one row should success', function (done) {
      var ep = new EventProxy;
      ep.all('batch_get_create_one_row_done', function () {
        batchGetRowFunc('table_client_test', [{
          partitionKey: 'batch_get_one',
          rowKey: "batch_get_one_row_key"
        }], function (err, data) {
          done();
        });
      });

      putRowFunc('table_client_test', 'batch_get_one', 'batch_get_one_row_key', columns, function (err, res) {
        should.not.exists(err);
        ep.emit('batch_get_create_one_row_done');
      });
    });

    it('batch get multi rows should success', function (done) {
      var ep = new EventProxy;

      ep.after('batch_get_create_multi_row_done', 100, function (list) {
        console.log('batch_get_create_multi_row_done');
        var keys = new Array();
        for (var i = 0; i < 100; i++) {
          var partitionKey = "batch_get_multi_" + i.toString();
          var rowKey = "batch_get_multi_row_key_" + i.toString();
          keys[i] = {
            partitionKey: partitionKey,
            rowKey: rowKey
          };
        }
        var dotime = 0;
        batchGetRowFunc('table_client_test', keys, {'strongConsistent': true}, function (err, data) {
          should.not.exists(err);
          done();
        });

      });

      for (var i = 0; i < 100; i++) {
        var partitionKey = "batch_get_multi_" + i.toString();
        var rowKey = "batch_get_multi_row_key_" + i.toString();
        putRowFunc('table_client_test', partitionKey, rowKey, columns, function (err, res) {
          should.not.exists(err);
          ep.emit('batch_get_create_multi_row_done', 'done');
        });
      }
    });

    it('batch write one rows should success', function (done) {
      var rows = [{
        key: {
          partitionKey: 'batch_write_one',
          rowKey: 'batch_write_row_key_one'
        },
        columns: {
          'name': 'Small Big',
          'age': 31
        },
        action: 'PUT' // action default is PUT
      }]

      batchWriteRowFunc('table_client_test', rows, function (err, data, info) {
        getRowFunc('table_client_test', 'batch_write_one', 'batch_write_row_key_one', true, null, function (err, data) {
          should.not.exists(err);
          checkRowValid(data, 'batch_write_one', 'batch_write_row_key_one', {
            'name': 'Small Big',
            'age': 31
          });
          done();
        });

      });
    });

    it('batch write/delete rows should success', function (done) {
      this.timeout(20000);
      var rows = [];
      var ep = new EventProxy();
      for (var i = 0; i < 25; i++) {
        rows[i] = {
          key: {
            partitionKey: 'batch_write_multi_' + i.toString(),
            rowKey: i.toString()
          },
          columns: {
            'name': 'column_value' + i.toString(),
            'age': i
          },
          action: 'PUT' // action default is PUT
        };
      }


      batchWriteRowFunc('table_client_test', rows, function (err, data, info) {
        for (var i = 0; i < 25; i++) {
          var pKey = 'batch_write_multi_' + i.toString();
          var rKey = i.toString();
          getRowFunc('table_client_test', pKey, rKey, true, null, function (err, data) {
            should.not.exists(err);
            var i = data.key.rowKey;
            checkRowValid(data, 'batch_write_multi_' + i, i, {
              'name': 'column_value' + i,
              'age': i
            });
            ep.emit('batch_write_multi_check', data.key.partitionKey);
          });

        }
        ep.after('batch_write_multi_check', 25, function (list) {
          ep.emit('batch_delete_start');
        });
      });

      ep.all('batch_delete_start', function () {
        for (var i = 0; i < 25; i++) {
          rows[i].columns = null;
          rows[i].action = 'DELETE';
        }
        batchWriteRowFunc('table_client_test', rows, function (err, data, info) {
          for (var i = 0; i < 25; i++) {
            var pKey = 'batch_write_multi_' + i.toString();
            var rKey = i.toString();
            getRowFunc('table_client_test', pKey, rKey, true, null, function (err, data) {
              shouldRowNotExists(data);
              ep.emit('batch_delete_multi_check', '');
            });
          }
        });
      });

      ep.after('batch_delete_multi_check', 25, function (list) {
        done();
      });

    });

    it('batch mixed write-delete should success', function (done) {
      this.timeout(20000);
      var ep = new EventProxy();
      var rows = [];
      for (var i = 0; i < 20; i++) {
        rows[i] = {
          key: {
            partitionKey: 'batch_write_multi_' + i.toString(),
            rowKey: i.toString()
          },
          columns: {
            'name': 'column_value' + i.toString(),
            'age': i
          },
          action: 'PUT' // action default is PUT
        };
      }
      for (var i = 0; i < 20; i += 2) {
        rows[i] = {
          key: {
            partitionKey: 'batch_mixed_delete_write_' + i.toString(),
            rowKey: i.toString()
          },
          columns: {
            'name': 'column_value' + i.toString(),
            'age': i
          },
          action: 'PUT' // action default is PUT
        };
      }

      batchWriteRowFunc('table_client_test', rows, function (err, data, info) {
        should.not.exists(err)
        for (var i = 0; i < 20; i += 2) {
          var pKey = 'batch_mixed_delete_write_' + i.toString();
          var rKey = i.toString();
          getRowFunc('table_client_test', pKey, rKey, true, null, function (err, data) {
            should.not.exists(err);
            var i = data.key.rowKey;
            checkRowValid(data, 'batch_mixed_delete_write_' + i, i, {
              'name': 'column_value' + i,
              'age': i
            });
            ep.emit('batch_mixed_write_check', data.key.partitionKey);
          });

        }
        ep.after('batch_mixed_write_check', 10, function () {
          ep.emit('batch_mixed_delete_start');
        });
      });

      ep.all('batch_mixed_delete_start', function () {
        for (var i = 0; i < 20; i++) {
          if (i % 2 == 0) {
            rows[i] = {
              key: {
                partitionKey: 'batch_mixed_delete_write_' + i.toString(),
                rowKey: i.toString()
              },
              action: 'DELETE' // action default is PUT
            };
          } else {
            rows[i] = {
              key: {
                partitionKey: 'batch_mixed_delete_write_' + i.toString(),
                rowKey: i.toString()
              },
              columns: {
                'name': 'column_value' + i.toString(),
                'age': i
              },
              action: 'PUT' // action default is PUT
            };
          }
        }
        batchWriteRowFunc('table_client_test', rows, function (err, data, info) {
          for (var i = 0; i < 20; i++) {
            var pKey = 'batch_mixed_delete_write_' + i.toString();
            var rKey = i.toString();
            getRowFunc('table_client_test', pKey, rKey, true, null, function (err, data) {
              should.not.exists(err);
              var i = data.key.rowKey;
              if (i % 2 == 0) {
                shouldRowNotExists(data);
              } else {
                checkRowValid(data, 'batch_mixed_delete_write_' + i, i, {
                  'name': 'column_value' + i,
                  'age': i
                });
              }
              ep.emit('batch_mixed_check', '');
            });
          }
        });
      });

      ep.after('batch_mixed_check', 20, function (list) {
        done();
      });
    });

    it('scan should success', function (done) {
      this.timeout(1000000);
      var zeroPadding = function (num, size) {
        var s = "000000000" + num.toString();
        return s.substr(s.length - size);
      };

      var ep = new EventProxy();
      var num =1000;
      for (var i = 0; i < num; i++) {
        var columns = {
          'name': 'column_value' + i.toString(),
          'age': i
        };
        setTimeout(function(index, columns){
              putRowFunc('table_client_test', 'scan_write_' + zeroPadding(index, 4), index.toString(), columns, function (err, data, info) {
              should.not.exists(err);
              ep.emit('scan_write_done', '');
            });
        }, 5 * i, i, columns);
      }

      ep.after('scan_write_done', num, function (list) {
        var start = 0;
        ScanAllFunc('table_client_test', function (err, data, info) {
          for (var i = 0; i < data.length; i++) {
            var index = i + start;
            checkRowValid(data[i], 'scan_write_' + zeroPadding(index, 4), index.toString(), {
              age: index,
              name: 'column_value' + index.toString()
            });
            ep.emit('scan_check_done', index);
          }
          if (info.nextStartKey) {
            start = parseInt(info.nextStartKey.rowKey)
          }
        });
      });

      ep.after('scan_check_done', num, function (list) {
        done();
      });
    });

    it('scan with endkey should success', function (done) {
      this.timeout(100000);
      var zeroPadding = function (num, size) {
        var s = "000000000" + num.toString();
        return s.substr(s.length - size);
      };

      var ep = new EventProxy();
      for (var i = 0; i < 100; i++) {
        var columns = {
          'name': 'column_value' + i.toString(),
          'age': i
        };

        (function(idx){
          putRowFunc('table_client_test', 'scan_write_' + zeroPadding(idx, 4), idx.toString(), columns, function (err, data, info) {
            should.not.exists(err);
            ep.emit('scan_endkey_write_done', idx);
          })
        })(i);
      }

      ep.after('scan_endkey_write_done', 100, function (list) {
        scanFunc('table_client_test', null, {
          partitionKey: 'scan_write_0050',
          rowKey: '50'
        }, function (err, data, info) {
          should.not.exists(err);
          should.equal(data.length, 50);

          for (var i = 0; i < data.length; i++) {
            var index = i;
            checkRowValid(data[i], 'scan_write_' + zeroPadding(index, 4), index.toString(), {
              age: index,
              name: 'column_value' + index.toString()
            });
          }
          done();
        });
      });
    });
  });
});
