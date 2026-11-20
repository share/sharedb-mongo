var expect = require('chai').expect;
var RequestDeduplicator = require('../src/request-deduplicator');

describe('RequestDeduplicator', function() {
  var deduplicator;
  var pendingFetches;

  beforeEach(function() {
    deduplicator = new RequestDeduplicator();
    pendingFetches = [];
  });

  function fetch(callback) {
    pendingFetches.push(callback);
  }

  function collectResults(count, callback) {
    var results = [];
    return function(error, result) {
      results.push({error: error, result: result});
      if (results.length === count) callback(results);
    };
  }

  it('shares one fetch between concurrent requests with the same key', function(done) {
    var callback = collectResults(2, function(results) {
      expect(results[0].result).to.eql([{v: 1}]);
      expect(results[1].result).to.eql([{v: 1}]);
      expect(results[0].result).not.to.equal(results[1].result);
      done();
    });
    deduplicator.run(['a', 1], fetch, callback);
    deduplicator.run(['a', 1], fetch, callback);

    expect(pendingFetches).to.have.length(1);
    pendingFetches[0](null, [{v: 1}]);
  });

  it('fetches separately for different keys', function() {
    deduplicator.run(['a', 1], fetch, function() {});
    deduplicator.run(['a', 2], fetch, function() {});
    expect(pendingFetches).to.have.length(2);
  });

  it('fetches again once the previous request has completed', function() {
    deduplicator.run(['a'], fetch, function() {});
    pendingFetches[0](null, []);
    deduplicator.run(['a'], fetch, function() {});
    expect(pendingFetches).to.have.length(2);
  });

  it('passes an error to every caller', function(done) {
    var error = new Error('fetch failed');
    var callback = collectResults(2, function(results) {
      expect(results[0].error).to.equal(error);
      expect(results[1].error).to.equal(error);
      done();
    });
    deduplicator.run(['a'], fetch, callback);
    deduplicator.run(['a'], fetch, callback);

    pendingFetches[0](error);
  });

  it('does not deduplicate a key that is not plain JSON', function() {
    var key = [new Date(0)];
    deduplicator.run(key, fetch, function() {});
    deduplicator.run(key, fetch, function() {});
    expect(pendingFetches).to.have.length(2);
  });

  it('refetches for joiners when the result is not plain JSON', function(done) {
    var leaderResult = {date: new Date(0)};
    var joinerResult = {date: new Date(0)};
    deduplicator.run(['a'], fetch, function(error, result) {
      expect(result).to.equal(leaderResult);
    });
    deduplicator.run(['a'], fetch, function(error, result) {
      expect(result).to.equal(joinerResult);
      done();
    });

    pendingFetches[0](null, leaderResult);
    setImmediate(function() {
      expect(pendingFetches).to.have.length(2);
      pendingFetches[1](null, joinerResult);
    });
  });

  it('does not leak the leader mutating its result into the joiners', function(done) {
    deduplicator.run(['a'], fetch, function(error, result) {
      result.v = 2;
    });
    deduplicator.run(['a'], fetch, function(error, result) {
      expect(result).to.eql({v: 1});
      done();
    });

    pendingFetches[0](null, {v: 1});
  });

  it('still calls back joiners when the leader callback throws', function(done) {
    deduplicator.run(['a'], fetch, function() {
      throw new Error('leader failed');
    });
    deduplicator.run(['a'], fetch, function(error, result) {
      expect(result).to.eql({v: 1});
      done();
    });

    expect(function() {
      pendingFetches[0](null, {v: 1});
    }).to.throw('leader failed');
  });
});
