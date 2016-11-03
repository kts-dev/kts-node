/**
 * Created by baibin on 2016/11/1.
 */

const Kts = require('../lib/kts.js');

const config = require('./config.js');

function BenchScan() {
}
var client = new Kts({
    endpoint: config.endpoint,
    accessKey: config.accessKey,
    secretKey: config.secretKey
});

var scan = function() {

    var options = {};
    if (arguments.length === 1) {
        options = {startKey: arguments[0]}
    }
    client.scan(config.tableName, options,  function (err, result, info) {
        if (err) {
            console.warn(err);
        } else {
            console.log(info);

            start_key = info.nextStartKey;
            if(info.hasOwnProperty('nextStartKey')) {
                console.log(start_key);
                scan( start_key )
            }
        }
    });
}
BenchScan.prototype.work = function() {
    scan()
}
module.exports = BenchScan;