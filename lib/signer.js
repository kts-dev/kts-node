'use strict';

const crypto = require('crypto');

const utils = {
  hash: function(str) {
    const hmac = crypto.createHash('sha256');
    hmac.update(str);
    return hmac.digest('base64');
  },
  hmac: function(str, key) {
    const hmac = crypto.createHmac('sha256', key);
    hmac.update(str);
    return hmac.digest('base64');
  }
};
/**
 *
 * @param options {{algorithm, accessKey, secretKey}}
 * @returns {{sign: sign}}
 */
function Signer(options) {
  if (!options.accessKey || !options.secretKey) {
    throw new Error('accessKey:' + options.accessKey + ',secretKey:' + options.secretKey + ' is illegal!');
  }
  this.algorithm = options.algorithm || 'KWS-HMAC-SHA256';
  this.accessKey = options.accessKey;
  this.secretKey = options.secretKey;
}

/**
 *
 * @param method
 * @param headers
 * @returns {string}
 */
Signer.prototype.sign = function(method, headers) {
  var signed_headers = ''; // headers need to sign
  for (var name in headers) {
    if (name === 'Host' || name === 'Connection') {
      continue;
    }
    if (signed_headers) {
      signed_headers += ';' + name;
    } else {
      signed_headers = name;
    }
  }

  var canonical_headers = ''; // [k=v]s, for k in signed headers
  for (var key in headers) {
    if (key === 'Host' || key === 'Connection') {
      continue;
    }
    if (headers.hasOwnProperty(key)) {
      canonical_headers += key.replace(/\s+/g, ' ') +
        ':' + (headers[key]).replace(/\s+/g, ' ') + '\n';
    }
  }

  var canonical_request = ''; // = method + uri + query + canonical_headers + signed_headers
  canonical_request += method + '\n';
  canonical_request += '\n'; // none
  canonical_request += '\n'; // none
  canonical_request += canonical_headers + '\n';
  canonical_request += signed_headers + '\n';
  canonical_request = utils.hash(canonical_request);

  var string_to_sign = ''; // = algorithm + datetime + canonical_request
  string_to_sign += this.algorithm + '\n';
  string_to_sign += headers['X-Kws-Date'] + '\n';
  string_to_sign += canonical_request;

  var signature = utils.hmac(string_to_sign, this.secretKey);

  return this.algorithm + ' AccessKey=' + this.accessKey + ',' +
    ' SignedHeaders=' + signed_headers + ',' +
    ' Signature=' + signature;
};

module.exports = Signer;