/**
 * Created by baibin on 2016/10/31.
 */
const Kts = require('../lib/kts.js');
const Benchmark = require('benchmark');
const config = require('./config.js');

var BenchPutOptions = {
    colomn_size: 1024,
    bench_mark_second : 3
};
var index = 0;

var generateKey = function() {
    index++;
    return 'key_' + index.toString();
};

var client = new Kts({
    endpoint: config.endpoint,
    accessKey: config.accessKey,
    secretKey: config.secretKey
});

var columns_value = 0;

var put = function() {
    var key = generateKey();
    var row = {
        key: {
            partitionKey: key,
            rowKey: 10001
        },
        columns: {
            'column': columns_value  // string
        }
    };
    client.putRow(config.tableName, row, function (err, result, info) {
        if (err) {
            console.log(err);
            throw err;
        }
        if ( (index % 1000) === 0) {
            console.log('baimushan ');
        }
     });
};

function BenchPut(options) {
    this.colomn_size = options.colomn_size;
    this.bench_mark_second = options.bench_mark_second;
    this.benchmark = new Benchmark('foo', {
        'onCycle': function(event) {
            console.log(String(event.target));
        },
        'onError': function(event) {
            console.log('onError');
        },
        'onAbort': function(event) {
            console.log('onAbort');
        },
        'fn': put,
        'maxTime' : this.bench_mark_second
    });
}

BenchPut.prototype.init = function() {
    var buf = Buffer.alloc(this.colomn_size);
    for (i = 0; i < this.colomn_size; ++i) {
        buf[i] = i;
    }
    columns_value = buf.toString()
};

BenchPut.prototype.work = function() {
    console.log('baimushan ');
    this.init();
    this.benchmark.run()
};

module.exports = BenchPut;