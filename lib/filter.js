'use strict';

const path = require('path');
const protoBuf = require('protobufjs');
const Long = protoBuf.Long;

var builder = protoBuf.newBuilder();
protoBuf.loadProtoFile(path.join(__dirname, '../proto/base.proto'));
protoBuf.loadProtoFile(path.join(__dirname, '../proto/filter.proto'), builder);
protoBuf.loadProtoFile(path.join(__dirname, '../proto/table.proto'), builder);
const Pb = builder.build('com.kingsoft.services.table.proto');

var inherits = require('util').inherits;
const utils = require('./base.js').utils;
const Encoder = require('./base.js').Encoder;
const Decoder = require('./base.js').Decoder;

if (typeof FilterType == "undefined") {
  var FilterType = {
    SingleColumnValueFilter: 1,
    FilterList: 2
  }
}

if (typeof FilterListType == "undefined") {
  var FilterListType = {
    MUST_PASS_ALL: 1,
    MUST_PASS_ONE: 2
  }
}

if (typeof CompareType == "undefined") {
  var CompareType = {
    LESS: 0,
    LESS_OR_EQUAL: 1,
    EQUAL: 2,
    NOT_EQUAL: 3,
    GREATER_OR_EQUAL: 4,
    GREATER: 5,
    NO_OP: 6
  }
}

function filterTypeToStr(type) {
  switch (type) {
    case FilterType.SingleColumnValueFilter:
      return "SingleColumnValueFilter";
    case FilterType.FilterList:
      return "FilterList";
    default:
      throw new Error('UnKnownFilterType:' + type);
  }
}


function filterListTypeToPB(type) {
  switch (type) {
    case FilterListType.MUST_PASS_ALL:
      return Pb.FilterList.Operator.MUST_PASS_ALL;
    case FilterListType.MUST_PASS_ONE:
      return Pb.FilterList.Operator.MUST_PASS_ONE;
    default:
      throw new Error('UnKnownFilterType:' + type);
  }
}


function compareTypeToPB(type) {
  switch (type) {
    case CompareType.LESS:
      return Pb.CompareType.kLess;
    case CompareType.LESS_OR_EQUAL:
      return Pb.CompareType.kLessOrEqual;
    case CompareType.EQUAL:
      return Pb.CompareType.kEqual;
    case CompareType.NOT_EQUAL:
      return Pb.CompareType.kNotEqual;
    case CompareType.GREATER_OR_EQUAL:
      return Pb.CompareType.kGreaterOrEqual;
    case CompareType.GREATER:
      return Pb.CompareType.kGreater;
    case CompareType.NO_OP:
      return Pb.CompareType.kNoOp;
    default:
      throw new Error('UnKnownCompareType:' + type);
  }
}

function parseCompareFilter(param) {
  // console.log(param.toString());
  if (!("column" in param)) { // not have columns
    throw utils.newError(-1, 'column', 'column ' + 'not exist');
  }
  if (!("compareType" in param)) {  // check compareType
    throw utils.newError(-1, 'column', 'name or type not exist');
  }
  if (Object.keys(param.column).length != 1) {  // check column
    throw utils.newError(-1, 'column', 'param.column length ' + Object.keys(param.column).length + 'is not 1');
  }

  var key;
  var value;
  for (var k  in param.column) {
    key = k;
    value = param.column[k];
  }

  var columnValue = Encoder.columnValue(value);
  var dataValueComparator = new Pb.DataValueComparator({
    value: columnValue
  }).toBuffer();
  var comparator = {
    name: "DataValueComparator",
    serialized_comparator: dataValueComparator
  };
  var compareFilter = new Pb.CompareFilter({
    compare_type: compareTypeToPB(param.compareType),
    comparator: comparator
  });
  return new Pb.SingleColumnValueFilter({
    column_name: key,
    compare_filter: compareFilter
  }).toBuffer();
}

function parseFilterList(param) {
  if (!("filters" in param)) {
    throw utils.newError(-1, 'FilterList', 'FilterList  has not filters ');
  }

  if (!("filterListType" in param)) {
    throw utils.newError(-1, 'FilterList', 'filterListType  has not filters ');
  }

  var filters = [];
  param.filters.forEach(function(filter) {
    filters.push(parse(filter));
  });
  return new Pb.FilterList({
    operator: filterListTypeToPB(param.filterListType),
    filters: filters
  }).toBuffer();
}

/**
 var request = {
          filterType : filter.FilterType.CompareFilter,
           compare : {
              column  : { age: 214748369},
              compareType : filter.CompareType.NOT_EQUAL
           }

 var request = {
       filterType : filter.FilterType.FilterList,
       filterListType : filter.FilterListType.MUST_PASS_ALL
       filters : [
              {
                  filterType : filter.FilterType.CompareFilter,
                  compare : {
                     column  : { age: 214748369},
                     compareType : filter.CompareType.NOT_EQUAL
                  }
              },
             {
                  filterType : filter.FilterType.CompareFilter,
                  compare : {
                     column  : { age: 214748369},
                     compareType : filter.CompareType.NOT_EQUAL
                  }
              }
       ]
    }
**/

function parse(param) {
  var res;
  if (!("filterType" in param)) {
    throw utils.newError(-1, 'filterType', 'filterType ' + 'not exist');
  }
  switch (param.filterType) {
    case FilterType.SingleColumnValueFilter:
      res = parseCompareFilter(param.compare);
      break;
    case FilterType.FilterList:
      res = parseFilterList(param);
      break;
    default:
      throw utils.newError(-1, 'filterType', 'filterType ' + param.filterType + '  not support');
  }
  return {
    name: filterTypeToStr(param.filterType),
    serialized_filter: res
  };
}

exports.FilterType = FilterType;
exports.CompareType = CompareType;
exports.FilterListType = FilterListType;
exports.parse = parse;

