var expect = require('expect.js');
var mongodb = require('mongodb');
var ShareDbMongo = require('../index');
var makeSortedQuery = require('sharedb-mingo-memory/make-sorted-query');

var mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';

function create(callback) {
  mongodb.connect(mongoUrl, function(err, mongo) {
    if (err) throw err;
    mongo.dropDatabase(function(err) {
      if (err) throw err;
      var db = ShareDbMongo({mongo: mongo});
      callback(null, db, mongo);
    });
  });
}

require('sharedb/test/db')(create, makeSortedQuery);

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
      this.db.commit('testcollection', 'foo', {v: 0, create: {}}, {}, function(err) {
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
      db.commit('system', 'test', {v: 0, create: {}}, {}, function(err) {
        expect(err).ok();
        db.getSnapshot('system', 'test', null, function(err) {
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

    it('queryPollDoc correctly filters on _id', function(done) {
      var snapshot = {type: 'json0', v: 1, data: {}, id: "test"};
      var db = this.db;

      var cases = [
        {query: {_id: "test"}, expectedHasDoc: true},
        {query: {_id: "nottest"}, expectedHasDoc: false},
        {query: {_id: /test/}, expectedHasDoc: true},
        {query: {_id: /nottest/}, expectedHasDoc: false},
        {query: {_id: {$in: ["test"]}}, expectedHasDoc: true},
        {query: {_id: {$in: ["nottest"]}}, expectedHasDoc: false}
      ];

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, function(err) {
        if (err) throw err;

        var numCallbacksWaiting = cases.length;
        for (var i = 0; i < cases.length; i++) {
          // create new scope for variables used in a callback defined inside
          (function() {
            var query = cases[i].query;
            var caseIndex = i;
            var expectedHasDoc = cases[i].expectedHasDoc;

            db.queryPollDoc(
              'testcollection', snapshot.id, query, null, function(err, hasDoc) {
                if (err) throw err;

                // include case index in test so that failing tests
                // point to a specific failures
                expect({caseIndex: caseIndex, hasDoc: hasDoc}).eql(
                  {caseIndex: caseIndex, hasDoc: expectedHasDoc});
                if (--numCallbacksWaiting === 0) done();
              });
          })();
        }
      });
    });

    it('$distinct should perform distinct operation', function(done) {
      var snapshots = [
        {type: 'json0', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', v: 1, data: {x: 3, y: 2}}
      ];
      var db = this.db;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], function(err) {
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
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], function(err) {
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
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], function(err) {
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
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], function(err) {
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

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, function(err) {
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
      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, function(err) {
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

