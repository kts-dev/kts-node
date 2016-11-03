var should = require('should');
const Kts = require('../lib/kts.js');
const config = require('./config.js');
var EventProxy = require('eventproxy');

describe('arguments_test.js', function () {
  var client = new Kts({
    endpoint: config.endpoint,
    accessKey: config.accessKey,
    secretKey: config.secretKey
  });

  describe('createTable', function () {
    it('tooShortTableName', function (done) {
      client.createTable('', function (err) {
        should.exist(err);
        done();
      });
    });

    it('illegalCharInTableName', function (done) {
      client.createTable('abcd#e', function (err) {
        should.exist(err);
        done();
      });
    });

    it('illegalTypeTableName', function (done) {
      client.createTable(true, function (err) {
        should.exist(err);
        done();
      });
    });

    it('exceptWhenNoCallback', function (done) {
      var exception = null;
      try {
        client.createTable(true);
      } catch (err) {
        exception = err;
      }
      should.exist(exception);
      done();
    });

    it('illegalSchema', function (done) {
      client.createTable('test', {
        schema: {}
      }, function (err) {
        should.exist(err);
        done();
      });
    });

    it('illegalPartitionKeyType', function (done) {
      client.createTable('test', {
        schema: {
          partitionKeyType: true
        }
      }, function (err) {
        should.exist(err);
        done();
      })
    });

    it('illegalRowKeyType', function (done) {
      client.createTable('test', {
        schema: {
          partitionKeyType: 'INT64',
          rowKeyType: 'DOUBLE'
        }
      }, function (err) {
        should.exist(err);
        done();
      });
    });

    it('illegalProvisionedThroughput', function (done) {
      client.createTable('test', {
        provisionedThroughput: {
          writeCapacityUnits: 100,
          readCapacityUnits: true
        }
      }, function (err) {
        should.exist(err);
        done();
      });
    })
  });

  describe('listTable', function () {
    it('ok', function (done) {
      client.listTables(function (err) {
        should.not.exists(err);
        done();
      });
    });
  });

  describe('describeTable', function () {
    it('illegalTableName', function(done){
      client.describeTable('abc#d', function(err){
        should.exist(err);
        done();
      });
    });

    it('notExists', function (done) {
      client.describeTable('notExists', function (err) {
        should.exist(err);
        done();
      });
    });
  });

  describe('putRow', function(){
    it('notExists', function(done){
      client.putRow('notExists', {
        key: {
          partitionKey: 'Key1',
          rowKey: 10001
        },
        columns: {
          name: 'Jack'
        }
      }, function(err){
        console.log(err);
        should.exist(err);
        done();
      });
    });
  });
});