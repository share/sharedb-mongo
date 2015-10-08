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

  describe('indexes', function() {
    it('adds ops index', function(done) {
      var mongo = this.mongo;
      this.db.commit('testcollection', 'foo', {v: 0, create: {}}, {}, function(err) {
        if (err) throw err;
        mongo.collection('ops_testcollection').indexInformation(function(err, indexes) {
          if (err) throw err;
          // Index for getting document(s) ops
          expect(indexes['d_1_v_1']).ok();
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
