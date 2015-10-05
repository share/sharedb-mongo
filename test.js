var expect = require('expect.js');
var mongodb = require('mongodb');
var ShareDbMongo = require('./index');
var assert = require('assert');

function create(callback) {
  mongodb.connect('mongodb://localhost:27017/test', function(err, mongo) {
    if (err) throw err;
    clear(mongo, function(err) {
      if (err) throw err;
      var db = ShareDbMongo({mongo: mongo});
      callback(null, db, mongo);
    });
  });
}
function clear(mongo, callback) {
  // Intentionally ignore errors, since drop returns an error if the
  // collection doesn't exist yet
  mongo.collection('testcollection').drop(function(err) {
    mongo.collection('ops_testcollection').drop(function(err) {
      callback();
    });
  });
}

require('sharedb/test/db')(create);

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

  describe('atomic commit', function() {
    it('commits a newly created doc', function(done) {
      var mongo = this.mongo;
      var opData = {v: 0, create: {type: 'json0', data: {x: 3}}};
      var snapshotData = {v: 1, type: 'json0', data: {x: 3}};
      this.db.commit('testcollection', 'foo', opData, snapshotData, function(err) {
        if (err) throw err;
        mongo.collection('testcollection').find({}).toArray(function(err, snapshotDocs) {
          if (err) throw err;
          mongo.collection('ops_testcollection').find({}).toArray(function(err, opDocs) {
            if (err) throw err;
            expect(snapshotDocs.length).equal(1);
            expect(opDocs.length).equal(1);
            expect(snapshotDocs[0]._o.equals(opDocs[0]._id)).ok();
            done();
          });
        });
      });
    });

    it('accepts one commit from two simultaneous create commits', function(done) {
      var mongo = this.mongo;
      var wait = 2;
      var commitCb = function(err) {
        if (err) throw err;
        if (--wait) return;
        mongo.collection('testcollection').find({}).toArray(function(err, snapshotDocs) {
          if (err) throw err;
          mongo.collection('ops_testcollection').find({}).toArray(function(err, opDocs) {
            if (err) throw err;
            expect(snapshotDocs.length).equal(1);
            expect(opDocs.length).equal(1);
            expect(snapshotDocs[0]._o.equals(opDocs[0]._id)).ok();
            done();
          });
        });
      };
      var opA = {v: 0, create: {type: 'json0', data: {x: 3}}};
      var snapshotA = {v: 1, type: 'json0', data: {x: 3}};
      var opB = {v: 0, create: {type: 'json0', data: {x: 5}}};
      var snapshotB = {v: 1, type: 'json0', data: {x: 5}};
      this.db.commit('testcollection', 'foo', opA, snapshotA, commitCb);
      this.db.commit('testcollection', 'foo', opB, snapshotB, commitCb);
    });

    it('accepts one commit from two simultaneous op commits', function(done) {
      var mongo = this.mongo;
      var wait = 2;
      var commitCb = function(err) {
        if (err) throw err;
        if (--wait) return;
        mongo.collection('testcollection').find({}).toArray(function(err, snapshotDocs) {
          if (err) throw err;
          mongo.collection('ops_testcollection').find({}).sort({v: -1}).toArray(function(err, opDocs) {
            if (err) throw err;
            expect(snapshotDocs.length).equal(1);
            expect(opDocs.length).equal(2);
            expect(snapshotDocs[0]._o.equals(opDocs[0]._id)).ok();
            expect(opDocs[0].o.equals(opDocs[1]._id)).ok();
            done();
          });
        });
      };
      var op0 = {v: 0, create: {type: 'json0', data: {x: 0}}};
      var snapshot0 = {v: 1, type: 'json0', data: {x: 0}};
      var db = this.db;
      db.commit('testcollection', 'foo', op0, snapshot0, function(err) {
        if (err) throw err;
        var opA = {v: 1, op: [{p: ['x'], na: 3}]};
        var snapshotA = {v: 2, type: 'json0', data: {x: 3}, _opLink: op0._id};
        var opB = {v: 1, op: [{p: ['x'], na: 5}]};
        var snapshotB = {v: 2, type: 'json0', data: {x: 5}, _opLink: op0._id};
        db.commit('testcollection', 'foo', opA, snapshotA, commitCb);
        db.commit('testcollection', 'foo', opB, snapshotB, commitCb);
      });
    });

    it('accepts one commit from two simultaneous delete commits', function(done) {
      var mongo = this.mongo;
      var wait = 2;
      var commitCb = function(err) {
        if (err) throw err;
        if (--wait) return;
        mongo.collection('testcollection').find({}).toArray(function(err, snapshotDocs) {
          if (err) throw err;
          mongo.collection('ops_testcollection').find({}).sort({v: -1}).toArray(function(err, opDocs) {
            if (err) throw err;
            expect(snapshotDocs.length).equal(0);
            expect(opDocs.length).equal(2);
            expect(opDocs[0].o.equals(opDocs[1]._id)).ok();
            done();
          });
        });
      };
      var op0 = {v: 0, create: {type: 'json0', data: {x: 0}}};
      var snapshot0 = {v: 1, type: 'json0', data: {x: 0}};
      var db = this.db;
      db.commit('testcollection', 'foo', op0, snapshot0, function(err) {
        if (err) throw err;
        var opA = {v: 1, del: true};
        var snapshotA = {v: 2, type: null, _opLink: op0._id};
        var opB = {v: 1, del: true};
        var snapshotB = {v: 2, type: null, _opLink: op0._id};
        db.commit('testcollection', 'foo', opA, snapshotA, commitCb);
        db.commit('testcollection', 'foo', opB, snapshotB, commitCb);
      });
    });
  });

  describe('indexes', function() {
    it('adds ops indexes', function(done) {
      var mongo = this.mongo;
      this.db.commit('testcollection', 'foo', {v: 0, create: {}}, {}, function(err) {
        if (err) throw err;
        mongo.collection('ops_testcollection').indexInformation(function(err, indexes) {
          if (err) throw err;
          // Index for getting document(s) ops
          expect(indexes['d_1_v_1']).ok();
          // Index for getting latest document(s) delete op
          expect(indexes['del_1_d_1_v_-1']).ok();
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
            var query = {$distinct: true, $field: 'y', $query: {}};
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
              $mapReduce: true,
              $map: function() {
                emit(this.player, this.score);
              },
              $reduce: function(key, values) {
                return values.reduce(function(t, s) {
                  return t + s;
                });
              },
              $query: {}
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
              $mapReduce: true,
              $map: function() {
                emit(this.player, this.score);
              },
              $reduce: function(key, values) {
                return values.reduce(function(t, s) {
                  return t + s;
                });
              },
              $query: {}
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
