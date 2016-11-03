/**
 * Created by baibin on 2016/10/31.
 */
const BenchPut = require('./bench_put.js');
const BenchGet = require('./bench_put.js');
const BenchScan = require('./bench_scan.js');

var bench_put = new BenchPut( {
    colomn_size: 1024,
    bench_mark_second : 10
});
var bench_get = new BenchGet( {
    colomn_size: 1024,
    bench_mark_second : 10
});
var scan = new BenchScan();


bench_put.work();
//bench_get.work();

//scan.work();