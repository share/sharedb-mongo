var expect = require('expect.js');
var mongodb = require('mongodb');
var ShareDbMongo = require('../index');
var getQuery = require('sharedb-mingo-memory/get-query');

var mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';

function create(callback) {
  var db = ShareDbMongo({mongo: function(shareDbCallback) {
    mongodb.connect(mongoUrl, function(err, mongo) {
      if (err) throw err;
      mongo.dropDatabase(function(err) {
        if (err) throw err;
        shareDbCallback(null, mongo);
        callback(null, db, mongo);
      });
    });
  }});
};

require('sharedb/test/db')({create: create, getQuery: getQuery});

describe('mongo db', function() {
  beforeEach(function(done) {
    var self = this;
    create(function(err, db, mongo) {
      if (err) throw err;
      self.db = db;
      self.mongo = mongo;
      done();
    });
  });

  afterEach(function(done) {
    this.db.close(done);
  });

  describe('indexes', function() {
    it('adds ops index', function(done) {
      var mongo = this.mongo;
      this.db.commit('testcollection', 'foo', {v: 0, create: {}}, {}, null, function(err) {
        if (err) throw err;
        mongo.collection('o_testcollection').indexInformation(function(err, indexes) {
          if (err) throw err;
          // Index for getting document(s) ops
          expect(indexes['d_1_v_1']).ok();
          // Index for checking committed op(s) by src and seq
          expect(indexes['src_1_seq_1_v_1']).ok();
          done()
        });
      });
    });
  });

  describe('security options', function() {
    it('does not allow editing the system collection', function(done) {
      var db = this.db;
      db.commit('system', 'test', {v: 0, create: {}}, {}, null, function(err) {
        expect(err).ok();
        db.getSnapshot('system', 'test', null, null, function(err) {
          expect(err).ok();
          done();
        });
      });
    });
  });

  describe('query', function() {
    // Run query tests for the types of queries supported by ShareDBMingo
    require('sharedb-mingo-memory/test/query')();

    it('does not allow $where queries', function(done) {
      this.db.query('testcollection', {$where: 'true'}, null, null, function(err, results) {
        expect(err).ok();
        done();
      });
    });

    it('queryPollDoc does not allow $where queries', function(done) {
      this.db.queryPollDoc('testcollection', 'somedoc', {$where: 'true'}, null, function(err) {
        expect(err).ok();
        done();
      });
    });

    it('$query is deprecated', function(done) {
      this.db.query('testcollection', {$query: {}}, null, null, function(err) {
        expect(err).ok();
        expect(err.code).eql(4106);
        done();
      });
    });

    it('only one collection operation allowed', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $aggregate: {}}, null, null, function(err) {
        expect(err).ok();
        expect(err.code).eql(4108);
        done();
      });
    });

    it('only one cursor operation allowed', function(done) {
      this.db.query('testcollection', {$count: true, $explain: true}, null, null, function(err) {
        expect(err).ok();
        expect(err.code).eql(4109);
        done();
      });
    });

    it('cursor transform can\'t run after collection operation', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $sort: {y: 1}}, null, null, function(err) {
        expect(err).ok();
        expect(err.code).eql(4110);
        done();
      });
    });

    it('cursor operation can\'t run after collection operation', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $count: true}, null, null, function(err) {
        expect(err).ok();
        expect(err.code).eql(4110);
        done();
      });
    });

    it('non-object $readPref should return error', function(done) {
      this.db.query('testcollection', {$readPref: true}, null, null, function(err) {
        expect(err).ok();
        expect(err.code).eql(4107);
        done();
      });
    });

    it('malformed $mapReduce should return error', function(done) {
      this.db.allowJSQueries = true; // required for $mapReduce
      this.db.query('testcollection', {$mapReduce: true}, null, null, function(err) {
        expect(err).ok();
        expect(err.code).eql(4107);
        done();
      });
    });

    describe('queryPollDoc correctly filters on _id', function(done) {
      var snapshot = {type: 'json0', v: 1, data: {}, id: "test"};

      beforeEach(function(done) {
        this.db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, done);
      });

      it('filter on id string that matches doc', function(done) {
        test.bind(this)({_id: 'test'}, true, done);
      });
      it('filter on id string that doesn\'t match doc', function(done) {
        test.bind(this)({_id: 'nottest'}, false, done);
      });
      it('filter on id regexp that matches doc', function(done) {
        test.bind(this)({_id: /test/}, true, done);
      });
      it('filter on id regexp that doesn\'t match doc', function(done) {
        test.bind(this)({_id: /nottest/}, false, done);
      });
      it('filter on id $in that matches doc', function(done) {
        test.bind(this)({_id: {$in: ['test']}}, true, done);
      });
      it('filter on id $in that doesn\'t match doc', function(done) {
        test.bind(this)({_id: {$in: ['nottest']}}, false, done);
      });

      // Intentionally inline calls to 'it' rather than place them
      // inside 'test' so that you can use Mocha's 'skip' or 'only'
      function test(query, expectedHasDoc, done) {
        this.db.queryPollDoc(
          'testcollection',
          snapshot.id,
          query,
          null,
          function(err, hasDoc) {
            if (err) done(err);
            expect(hasDoc).eql(expectedHasDoc);
            done();
          }
        );
      };
    });

    it('$distinct should perform distinct operation', function(done) {
      var snapshots = [
        {type: 'json0', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', v: 1, data: {x: 3, y: 2}}
      ];
      var db = this.db;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            var query = {$distinct: {field: 'y'}};
            db.query('testcollection', query, null, null, function(err, results, extra) {
              if (err) throw err;
              expect(extra).eql([1, 2]);
              done();
            });
          });
        });
      });
    });

    it('$aggregate should perform aggregate command', function(done) {
      var snapshots = [
        {type: 'json0', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', v: 1, data: {x: 3, y: 2}}
      ];
      var db = this.db;
      db.allowAggregateQueries = true;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            var query = {$aggregate: [
              {$group: {_id: '$y', count: {$sum: 1}}},
              {$sort: {count: 1}}
            ]};
            db.query('testcollection', query, null, null, function(err, results, extra) {
              if (err) throw err;
              expect(extra).eql([{_id: 1, count: 1}, {_id: 2, count: 2}]);
              done();
            });
          });
        });
      });
    });

    it('does not let you run $aggregate queries without options.allowAggregateQueries', function(done) {
      var query = {$aggregate: [
        {$group: {_id: '$y',count: {$sum: 1}}},
        {$sort: {count: 1}}
      ]};
      this.db.query('testcollection', query, null, null, function(err, results) {
        expect(err).ok();
        done();
      });
    });

    it('does not allow $mapReduce queries by default', function(done) {
      var snapshots = [
        {type: 'json0', v: 1, data: {player: 'a', round: 1, score: 5}},
        {type: 'json0', v: 1, data: {player: 'a', round: 2, score: 7}},
        {type: 'json0', v: 1, data: {player: 'b', round: 1, score: 15}}
      ];
      var db = this.db;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            var query = {
              $mapReduce: {
                map: function() {
                  emit(this.player, this.score);
                },
                reduce: function(key, values) {
                  return values.reduce(function(t, s) {
                    return t + s;
                  });
                }
              }
            };
            db.query('testcollection', query, null, null, function(err) {
              expect(err).ok();
              done();
            });
          });
        });
      });
    });

    it('$mapReduce queries should work when allowJavaScriptQuery == true', function(done) {
      var snapshots = [
        {type: 'json0', v: 1, data: {player: 'a', round: 1, score: 5}},
        {type: 'json0', v: 1, data: {player: 'a', round: 2, score: 7}},
        {type: 'json0', v: 1, data: {player: 'b', round: 1, score: 15}}
      ];
      var db = this.db;
      db.allowJSQueries = true;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            var query = {
              $mapReduce: {
                map: function() {
                  emit(this.player, this.score);
                },
                reduce: function(key, values) {
                  return values.reduce(function(t, s) {
                    return t + s;
                  });
                }
              }
            };
            db.query('testcollection', query, null, null, function(err, results, extra) {
              if (err) throw err;
              expect(extra).eql([{_id: 'a', value: 12}, {_id: 'b', value: 15}]);
              done();
            });
          });
        });
      });
    });
  });
});

describe('mongo db connection', function() {
  describe('via url string', function() {
    beforeEach(function(done) {
      this.db = ShareDbMongo({mongo: mongoUrl});

      // This will enqueue the callback, testing the 'pendingConnect'
      // logic.
      this.db.getDbs(function(err, mongo, mongoPoll) {
        if (err) throw err;
        mongo.dropDatabase(function(err) {
          if (err) throw err;
          done();
        });
      });
    });

    afterEach(function(done) {
      this.db.close(done);
    });

    it('commit and query', function(done) {
      var snapshot = {type: 'json0', v: 1, data: {}, id: "test"};
      var db = this.db;

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) throw err;
        db.query('testcollection', {}, null, null, function(err, results) {
          if (err) throw err;
          expect(results).eql([snapshot]);
          done();
        });
      });
    });
  });

  describe('via url string with mongoPoll and pollDelay option', function() {
    beforeEach(function(done) {
      this.pollDelay = 1000;
      this.db = ShareDbMongo({mongo: mongoUrl, mongoPoll: mongoUrl, pollDelay: this.pollDelay});
      done();
    });

    afterEach(function(done) {
      this.db.close(done);
    });

    it('delays queryPoll but not commit', function(done) {
      var db = this.db;
      var pollDelay = this.pollDelay;

      var snapshot = {type: 'json0', v: 1, data: {}, id: "test"};
      var timeBeforeCommit = new Date;
      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        expect((new Date) - timeBeforeCommit).lessThan(pollDelay);

        var timeBeforeQuery = new Date;
        db.queryPoll('testcollection', {}, null, function(err, results) {
          expect(results.length).eql(1);
          expect((new Date) - timeBeforeQuery).greaterThan(pollDelay);
          done();
        });
      });
    });
  });
});

describe('parse query', function() {
  var parseQuery = ShareDbMongo._parseQuery;
  var makeQuerySafe = ShareDbMongo._makeQuerySafe;

  var needToAddTypeNeNull = function(query) {
    var queryWithTypeNeNull = shallowClone(query);
    queryWithTypeNeNull._type = {$ne: null};
    var parsedQuery = parseQuery(query);
    makeQuerySafe(parsedQuery);
    expect(parsedQuery.query).eql(queryWithTypeNeNull);
  };

  var queryIsSafeAsIs = function(query) {
    var parsedQuery = parseQuery(query);
    makeQuerySafe(parsedQuery);
    expect(parsedQuery.query).eql(query);
  };

  describe('adds _type: {$ne: null} when necessary', function() {
    it('basic', function() {
      needToAddTypeNeNull({});
      needToAddTypeNeNull({foo: null});
      queryIsSafeAsIs({foo: 1});
      needToAddTypeNeNull({foo: {$bitsAllSet: 1}}); // We don't try to analyze $bitsAllSet
    });

    it('$ne', function() {
      needToAddTypeNeNull({foo: {$ne: 1}});
      queryIsSafeAsIs({foo: {$ne: 1}, bar: 1});
      queryIsSafeAsIs({foo: {$ne: null}});
    });

    it('comparisons', function() {
      queryIsSafeAsIs({foo: {$gt: 1}});
      queryIsSafeAsIs({foo: {$gte: 1}});
      queryIsSafeAsIs({foo: {$lt: 1}});
      queryIsSafeAsIs({foo: {$lte: 1}});
      queryIsSafeAsIs({foo: {$gte: 2, $lte: 5}});
      needToAddTypeNeNull({foo: {$gte: null, $lte: null}});
    });

    it('$exists', function() {
      queryIsSafeAsIs({foo: {$exists: true}});
      needToAddTypeNeNull({foo: {$exists: false}});
      queryIsSafeAsIs({foo: {$exists: true}, bar: {$exists: false}});
    });

    it('$not', function() {
      needToAddTypeNeNull({$not: {foo: 1}});
      needToAddTypeNeNull({$not: {foo: null}}); // We don't try to analyze $not
    });

    it('$in', function() {
      queryIsSafeAsIs({foo: {$in: [1, 2, 3]}});
      needToAddTypeNeNull({foo: {$in: [null, 2, 3]}});
      queryIsSafeAsIs({foo: {$in: [null, 2, 3]}, bar: 1});
    })

    it('top-level $and', function() {
      queryIsSafeAsIs({$and: [{foo: {$ne: null}}, {bar: {$ne: null}}]});
      queryIsSafeAsIs({$and: [{foo: {$ne: 1}}, {bar: {$ne: null}}]});
      needToAddTypeNeNull({$and: [{foo: {$ne: 1}}, {bar: {$ne: 1}}]});
    });

    it('top-level $or', function() {
      queryIsSafeAsIs({$or: [{foo: {$ne: null}}, {bar: {$ne: null}}]});
      needToAddTypeNeNull({$or: [{foo: {$ne: 1}}, {bar: {$ne: null}}]});
      needToAddTypeNeNull({$or: [{foo: {$ne: 1}}, {bar: {$ne: 1}}]});
    });

    it('malformed queries', function() {
      // if we don't understand the query, definitely don't mark it as
      // "safe as is"
      needToAddTypeNeNull({$or: {foo: 3}});
      needToAddTypeNeNull({foo: {$or: 3}});
      needToAddTypeNeNull({$not: [1, 2]});
      needToAddTypeNeNull({foo: {$bad: 1}});
      needToAddTypeNeNull({$bad: [2, 3]});
      needToAddTypeNeNull({$and: [[{foo: 1}]]});
    });
  });
});

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}
