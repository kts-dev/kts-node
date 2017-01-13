/**
 * Created by baibin on 2016/12/30.
 */
var should = require('should');
const filter = require('../lib/filter.js');
const Kts = require('../lib/kts.js');
const config = require('./config.js');
var EventProxy = require('eventproxy');
var assert = require("assert");

var Promise = require('bluebird');

describe('filter.js', function() {

  var client = new Kts({
    endpoint: config.endpoint,
    accessKey: config.accessKey,
    secretKey: config.secretKey,
    tableName: config.tableName
  });


  var partionKey = "partion_key";

  var columns = {
    'name': 'Jack',  // string
    'age': 30,  // int
    'weight': 62.3,  // double, not recommend
    'is_vip': true  // bool
  };

  var res;

  var putRowFunc = function(partitionKey, columns, callback) {
    client.putRow(config.tableName, {
      key: {
        partitionKey: partitionKey,
        rowKey: 10001
      },
      columns: columns
    }, callback);
  };

  var batchWriteFunc = function(row, callback) {
    client.batchWriteRow(config.tableName, row, callback);
  };

  var batchGetFunc = function(filter, callback) {
    client.batchGetRow(config.tableName, [{
      partitionKey: 'key_1',
      rowKey: 10001
    }, {
      partitionKey: 'key_2',
      rowKey: 10001
    }], {
      strongConsistent: true,
      filter: filter
    }, callback);
  };

  var getRowFunc = function(partitionKey, filter, callback) {
    res = client.getRow(config.tableName, {
      key: {
        partitionKey: partitionKey,
        rowKey: 10001
      },
      strongConsistent: true,
      filter: filter
    }, callback);

  };
  var scanFunc = function(filter, callback) {
    client.scan(config.tableName, {
      'startKey': {
        partitionKey: 'key',
        rowKey: 10001
        //rowKey: 'start_row_key'
      },
      'endKey': {
        partitionKey: 'key_5',
        rowKey: 10001
        //rowKey: 'start_row_key'
      },
      filter: filter
    }, callback);

  };

  var deleteRowFunc = function(partitionKey, callback) {
    client.deleteRow(config.tableName, {
      partitionKey: partitionKey
    }, callback);
  };

  var begin = function() {
    return new Promise(function(resolve, reject) {
      res = null;
      putRowFunc(partionKey, columns, function(err) {
        if (err) {
          reject(err);
        } else {
          resolve("begin");
        }
      });
    });
  };

  var end = function() {
    return function() {
      return new Promise(function(resolve, reject) {
        deleteRowFunc(partionKey, function(err) {
          if (err) {
            reject(err);
          } else {
            setTimeout(function() {
              //console.log( "begin" );
              resolve("end");
            }, 500);

          }

        });
      });
    }
  };

  var base_filter = {
    filterType: filter.FilterType.SingleColumnValueFilter,
    compare: {
      column: {age: 30},
      compareType: filter.CompareType.EQUAL
    }
  };

  var get_check = function(compare_type, column, res) {
    return function() {
      var equal = base_filter;
      equal.compare.column = column;
      equal.compare.compareType = compare_type;
      return new Promise(function(resolve, reject) {
        getRowFunc(partionKey, equal, function(err, result, info) {
          if (err) {
            reject(err);
          } else {
          //   console.log(result);
            if (res) {
              assert.notEqual(Object.keys(result.columns).length, 0);
            } else {
              assert.equal(Object.keys(result.columns).length, 0);
            }
            resolve(result);
          }
        });
      });

    }
  };


  var createTable = function() {
    return new Promise(function(resolve, reject) {
      client.createTable(config.tableName, {
        schema: {
          partitionKeyType: 'STRING',
          rowKeyType: 'INT64'
        },
        provisionedThroughput: {
          readCapacityUnits: 5000,
          writeCapacityUnits: 5000
        }
      }, function(err, result, info) {
        if (err) {
          console.warn(err);
          reject(err);
        } else {
          setTimeout(function() {
            resolve("createTable");
          }, 1000);
        }
      });
    });
  };
  var deleteTable = function() {
    return new Promise(function(resolve, reject) {
      client.deleteTable(config.tableName, function(err, result, info) {
        if (err) {
          console.warn(err);
          reject(err);
        } else {
          setTimeout(function() {
            resolve("deleteTable");
          }, 1000);
        }
      });
    });
  };

  before(function() {
    // runs before all tests in this filter
    createTable().then(
      function() {
        console.log("create table success");
      }
    );
  });

  after(function() {
    // runs after all tests in this block
    deleteTable().then(
      function() {
        console.log("delete table success");
      }
    );
  });


  describe('base_fail_case', function() {
    it('base_fail', function() {
      //filterType not include
      try {
        var res = filter.parse({});
      } catch (err) {
        //     console.log(err);
        //return noticeError(err, callback);
      }

      //filterType not exist
      try {
        var res = filter.parse({filterType: 3});
      } catch (err) {
        //  console.log(err);
        //return noticeError(err, callback);
      }
    });

  });


  describe('get', function() {
    it('get_int_64', function(done) {

      begin()

        .then(get_check(filter.CompareType.EQUAL, {age: 20}, false))
        .then(get_check(filter.CompareType.EQUAL, {age: 30}, true))
        .then(get_check(filter.CompareType.NOT_EQUAL, {age: 20}, true))
        .then(get_check(filter.CompareType.NOT_EQUAL, {age: 30}, false))

        .then(get_check(filter.CompareType.LESS, {age: 30}, false))
        .then(get_check(filter.CompareType.LESS, {age: 40}, true))
        .then(get_check(filter.CompareType.LESS, {age: 20}, false))

        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {age: 30}, true))
        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {age: 40}, true))
        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {age: 20}, false))

        .then(get_check(filter.CompareType.GREATER_OR_EQUAL, {age: 30}, true))
        .then(get_check(filter.CompareType.GREATER_OR_EQUAL, {age: 20}, true))
        .then(get_check(filter.CompareType.GREATER_OR_EQUAL, {age: 40}, false))

        .then(get_check(filter.CompareType.GREATER, {age: 30}, false))
        .then(get_check(filter.CompareType.GREATER, {age: 20}, true))
        .then(get_check(filter.CompareType.GREATER, {age: 40}, false))

        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {age: 1000000000}, true))
        .then(get_check(filter.CompareType.EQUAL, {age: -1000}, false))
        .then(get_check(filter.CompareType.NOT_EQUAL, {age: -10000}, true))
        .then(get_check(filter.CompareType.LESS, {age: -100000}, false))
        .then(get_check(filter.CompareType.GREATER, {age: -10}, true))
        .then(get_check(filter.CompareType.GREATER, {age: 0}, true))

        .then(end)
        .then(function() {
          done();
        })
        .catch(done);

    });

    it('get_bool', function(done) {

      begin()
        .then(get_check(filter.CompareType.EQUAL, {is_vip: false}, false))
        .then(get_check(filter.CompareType.EQUAL, {is_vip: true}, true))
        .then(get_check(filter.CompareType.NOT_EQUAL, {is_vip: false}, true))
        .then(get_check(filter.CompareType.NOT_EQUAL, {is_vip: true}, false))

        .then(end)
        .then(function() {
          done();
        })
        .catch(done);

    });

    it('get_string', function(done) {
      begin()
        .then(get_check(filter.CompareType.EQUAL, {name: "Bai"}, false))
        .then(get_check(filter.CompareType.EQUAL, {name: "Jack"}, true))
        .then(get_check(filter.CompareType.NOT_EQUAL, {name: "Jack"}, false))
        .then(get_check(filter.CompareType.NOT_EQUAL, {name: "Bai"}, true))

        .then(get_check(filter.CompareType.LESS, {name: "Jack"}, false))
        .then(get_check(filter.CompareType.LESS, {name: "Wade"}, true))
        .then(get_check(filter.CompareType.LESS, {name: "Jac"}, false))

        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {name: "Jack"}, true))
        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {name: "Wade"}, true))
        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {name: "Jac"}, false))

        .then(get_check(filter.CompareType.GREATER_OR_EQUAL, {name: "Jack"}, true))
        .then(get_check(filter.CompareType.GREATER_OR_EQUAL, {name: "Jac"}, true))
        .then(get_check(filter.CompareType.GREATER_OR_EQUAL, {name: "Wade"}, false))

        .then(get_check(filter.CompareType.GREATER, {name: "Jack"}, false))
        .then(get_check(filter.CompareType.GREATER, {name: "Jac"}, true))
        .then(get_check(filter.CompareType.GREATER, {name: "Wade"}, false))

        .then(get_check(filter.CompareType.EQUAL, {name: " "}, false))
        .then(get_check(filter.CompareType.NOT_EQUAL, {name: " "}, true))
        .then(get_check(filter.CompareType.GREATER, {name: " "}, true))
        .then(get_check(filter.CompareType.GREATER_OR_EQUAL, {name: " "}, true))
        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {name: " "}, false))
        .then(get_check(filter.CompareType.LESS_OR_EQUAL, {name: "                                                                                                      "}, false))

        .then(end)
        .then(function() {
          done();
        })
        .catch(done);
    });

    // double only support two type
    it('get_double', function(done) {
      begin()

        .then(get_check(filter.CompareType.LESS, {weight: 62.2}, false))
        .then(get_check(filter.CompareType.LESS, {weight: 62.4}, true))

        .then(get_check(filter.CompareType.GREATER, {weight: 62.4}, false))
        .then(get_check(filter.CompareType.GREATER, {weight: 62.2}, true))


        .then(get_check(filter.CompareType.GREATER, {weight: -62.2}, true))
        .then(get_check(filter.CompareType.GREATER, {weight: 1.000000001}, true))
        .then(get_check(filter.CompareType.LESS, {weight: -62.2}, false))
        .then(get_check(filter.CompareType.LESS, {weight: -0.1}, false))

        .then(end)
        .then(function() {
          done();
        })
        .catch(done);
    });

  });

  var rowPut = [{
    key: {
      partitionKey: 'key_1',
      rowKey: 10001
    },
    columns: {
      'name': 'Jack',  // string
      'age': 30,  // int
      'weight': 62.3,  // double, not recommend
      'is_vip': true  // bool
    },
    action: 'PUT' // action default is PUT
  }, {
    key: {
      partitionKey: 'key_2',
      rowKey: 10001
    },
    columns: {
      'name': 'Lack',  // string
      'age': 20,  // int
      'weight': 62.3,  // double, not recommend
      'is_vip': false  // bool
    },
    action: 'PUT' // action default is PUT
  }];

  var rowDelete = [{
    key: {
      partitionKey: 'key_1',
      rowKey: 10001
    },
    action: 'DELETE'
  }, {
    key: {
      partitionKey: 'key_2',
      rowKey: 10001
    },
    action: 'DELETE'
  }];


  var begin_multi = function() {
    return new Promise(function(resolve, reject) {
      batchWriteFunc(rowPut, function(err) {
        if (err) {
          reject(err);
        } else {
          setTimeout(function() {
            //console.log( "begin" );
            resolve("begin");
          }, 500);
        }
      });
    });
  };

  var end_multi = function() {
    return new Promise(function(resolve, reject) {
      batchWriteFunc(rowDelete, function(err) {
        if (err) {
          reject(err);
        } else {
          setTimeout(function() {
            // console.log( "end" );
            resolve("end");
          }, 300);
        }
      });
    });
  };


  var myfilter = {
    filterType: filter.FilterType.FilterList,
    filterListType: filter.FilterListType.MUST_PASS_ONE,
    filters: [
      {
        filterType: filter.FilterType.SingleColumnValueFilter,
        compare: {
          column: {age: 30},
          compareType: filter.CompareType.EQUAL
        }
      },
      {
        filterType: filter.FilterType.FilterList,
        filterListType: filter.FilterListType.MUST_PASS_ONE,
        filters: [
          {
            filterType: filter.FilterType.SingleColumnValueFilter,
            compare: {
              column: {name: "Haha"},
              compareType: filter.CompareType.EQUAL
            }
          },
          {
            filterType: filter.FilterType.SingleColumnValueFilter,
            compare: {
              column: {is_vip: true},
              compareType: filter.CompareType.EQUAL
            }
          }
        ]
      }
    ]
  };

  var float_filter_list = {
    filterType: filter.FilterType.FilterList,
    filterListType: filter.FilterListType.MUST_PASS_ONE,
    filters: [
      {
        filterType: filter.FilterType.SingleColumnValueFilter,
        compare: {
          column: {weight: 1.2},
          compareType: filter.CompareType.GREATER
        }
      },
      {
        filterType: filter.FilterType.SingleColumnValueFilter,
        compare: {
          column: {is_vip: true},
          compareType: filter.CompareType.EQUAL
        }
      }
    ]
  };

  function deepCopy(o, c) {
    var c = c || {}
    for (var i in o) {
      if (typeof o[i] === 'object') {
        //要考虑深复制问题了
        if (o[i].constructor === Array) {
          //这是数组
          c[i] = []
        } else {
          //这是对象
          c[i] = {}
        }
        deepCopy(o[i], c[i])
      } else {
        c[i] = o[i]
      }
    }
    return c
  }


  var changeFilter = function(filterListType1, filterListType2, compareType1, value1, compareType2, value2, compareType3, value3) {
    var base = deepCopy(myfilter, base);
    base.filterListType = filterListType1;
    base.filters[1].filterListType = filterListType2;

    base.filters[0].compare.compareType = compareType1;
    base.filters[0].compare.column.age = value1;

    base.filters[1].filters[0].compare.compareType = compareType2;
    base.filters[1].filters[0].compare.column.name = value2;
    base.filters[1].filters[1].compare.compareType = compareType3;
    base.filters[1].filters[1].compare.column.is_vip = value3;
    // console.log(myfilter);
    // console.log(base);
    return base;
  };

  var scanCheck = function(filter, num) {
    return function() {
      return new Promise(function(resolve, reject) {
        scanFunc(filter, function(err, result, info) {
          if (err) {
            reject(err);
          } else {
            //console.log(result);
            assert.equal(result.length, num);
            resolve("scanCheck");

          }
        });
      });
    }
  };

  describe('scan', function() {
    it('scan', function(done) {

      begin_multi()
        .then(scanCheck(null, 2))

        .then(scanCheck(changeFilter(filter.FilterListType.MUST_PASS_ALL, filter.FilterListType.MUST_PASS_ALL,
          filter.CompareType.EQUAL, 30,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, true), 1))


        .then(scanCheck(changeFilter(filter.FilterListType.MUST_PASS_ONE, filter.FilterListType.MUST_PASS_ALL,
          filter.CompareType.NOT_EQUAL, 10,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, true), 2))

        .then(scanCheck(changeFilter(filter.FilterListType.MUST_PASS_ONE, filter.FilterListType.MUST_PASS_ONE,
          filter.CompareType.EQUAL, 10,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, false), 2))

        .then(scanCheck(changeFilter(filter.FilterListType.MUST_PASS_ALL, filter.FilterListType.MUST_PASS_ONE,
          filter.CompareType.NOT_EQUAL, 10,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, false), 2))
        .then(end_multi)
        .then(function() {
          done()
        })
        .catch(done);

    });
  });

  var batchGetCheck = function(filter, num) {
    return function() {
      return new Promise(function(resolve, reject) {
        batchGetFunc(filter, function(err, result, info) {
          if (err) {
            reject(err);
          } else {
            var size = 0;
            result.forEach(function(obj) {
              //  console.log(obj);
              if (Object.keys(obj.columns).length > 0) {
                size++;
              }
            });
            assert.equal(size, num);
            resolve("batchGetFunc");
          }
        });
      });
    }
  };


  describe('batchGetCheck', function() {
    it('batchGetCheck', function(done) {

      begin_multi()
        .then(batchGetCheck(null, 2))

        .then(batchGetCheck(changeFilter(filter.FilterListType.MUST_PASS_ALL, filter.FilterListType.MUST_PASS_ALL,
          filter.CompareType.EQUAL, 30,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, true), 1))

        .then(batchGetCheck(changeFilter(filter.FilterListType.MUST_PASS_ONE, filter.FilterListType.MUST_PASS_ALL,
          filter.CompareType.NOT_EQUAL, 10,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, true), 2))

        .then(batchGetCheck(changeFilter(filter.FilterListType.MUST_PASS_ONE, filter.FilterListType.MUST_PASS_ONE,
          filter.CompareType.EQUAL, 10,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, false), 2))

        .then(batchGetCheck(changeFilter(filter.FilterListType.MUST_PASS_ALL, filter.FilterListType.MUST_PASS_ONE,
          filter.CompareType.NOT_EQUAL, 10,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, false), 2))


        .then(batchGetCheck(changeFilter(filter.FilterListType.MUST_PASS_ALL, filter.FilterListType.MUST_PASS_ONE,
          filter.CompareType.NOT_EQUAL, 10,
          filter.CompareType.EQUAL, "Jack",
          filter.CompareType.EQUAL, false), 2))

        .then(batchGetCheck(float_filter_list, 2))
        .then(end_multi)
        .then(function() {
          done()
        })
        .catch(done);

    });
  });
});


