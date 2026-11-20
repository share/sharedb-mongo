var expect = require('chai').expect;
var ObjectId = require('../mongodb').ObjectId;
var stringifyPlainJson = require('../src/stringify-plain-json');

describe('stringifyPlainJson', function() {
  var nullPrototypeObject = Object.create(null);
  nullPrototypeObject.a = 1;

  [
    {name: 'null', value: null, json: 'null'},
    {name: 'zero', value: 0, json: '0'},
    {name: 'a numeric string', value: '0', json: '"0"'},
    {name: 'false', value: false, json: 'false'},
    {name: 'an empty array', value: [], json: '[]'},
    {name: 'an empty object', value: {}, json: '{}'},
    {name: 'an object with a null prototype', value: nullPrototypeObject, json: '{"a":1}'},
    {
      name: 'nested plain data',
      value: {a: [1, 'b', {c: null}], d: true},
      json: '{"a":[1,"b",{"c":null}],"d":true}'
    }
  ].forEach(function(testCase) {
    it('serializes ' + testCase.name, function() {
      expect(stringifyPlainJson(testCase.value)).to.equal(testCase.json);
    });
  });

  var circular = {};
  circular.self = circular;
  var sparse = [1];
  sparse[2] = 3;
  function ClassInstance() {}

  [
    {name: 'undefined', value: undefined},
    {name: 'a function', value: function() {}},
    {name: 'a symbol', value: Symbol.iterator},
    {name: 'NaN', value: NaN},
    {name: 'Infinity', value: Infinity},
    {name: '-Infinity', value: -Infinity},
    {name: 'a Date', value: new Date()},
    {name: 'an ObjectId', value: new ObjectId()},
    {name: 'a Map', value: new Map()},
    {name: 'a class instance', value: new ClassInstance()},
    {name: 'an object with toJSON', value: {toJSON: function() {
      return 'a';
    }}},
    {name: 'a circular object', value: circular},
    {name: 'a sparse array', value: sparse},
    {name: 'an undefined property', value: {a: undefined}},
    {name: 'a nested non-plain value', value: {a: [{b: new Date()}]}}
  ].forEach(function(testCase) {
    it('rejects ' + testCase.name, function() {
      expect(stringifyPlainJson(testCase.value)).to.equal(undefined);
    });
  });

  it('rethrows an error that is not about serializability', function() {
    var value = {
      get a() {
        throw new RangeError('getter failed');
      }
    };
    expect(function() {
      stringifyPlainJson(value);
    }).to.throw(RangeError, 'getter failed');
  });
});
