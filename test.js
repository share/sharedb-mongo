var mongodb = require('mongodb');
var LiveDbMongo = require('./index');
var assert = require('assert');

function clear(callback) {
  mongodb.connect('mongodb://localhost:27017/test', function(err, mongo) {
    if (err) throw err;
    mongo.dropCollection('testcollection', function() {
      mongo.dropCollection('testcollection_ops', function() {
        mongo.close(callback);
      });
    });
  });
}

function create(callback) {
  clear(function() {
    callback(LiveDbMongo('mongodb://localhost:27017/test'));
  });
}

describe('mongo', function() {
  afterEach(clear);

  describe('raw', function() {
    beforeEach(function(done) {
      var self = this;
      mongodb.connect('mongodb://localhost:27017/test', function(err, mongo) {
        self.mongo = mongo;
        create(function(db) {
          self.db = db;
          done();
        });
      });
    });

    afterEach(function(done) {
      this.mongo.close(done);
    });

    it('adds an index for ops', function(done) {
      var mongo = this.mongo;
      create(function(db) {
        db.writeOp('testcollection', 'foo', {v: 0, create: {type: 'json0'}}, function(err) {
          mongo.collection('testcollection_ops').indexInformation(function(err, indexes) {
            if (err) throw err;

            // We should find an index with [[ 'name', 1 ], [ 'v', 1 ]]
            for (var name in indexes) {
              var idx = indexes[name];
              if (JSON.stringify(idx) === '[["name",1],["v",1]]') {
                return done();
              }
            }

            throw Error("Could not find index in ops db - " + (JSON.stringify(indexes)));
          });
        });
      });
    });

    it('does not allow editing the system collection', function(done) {
      var db = this.db;
      db.writeSnapshot('system', 'test', {type: 'json0', v: 5, m: {}, data: {x: 5}}, function(err) {
        assert.ok(err);
        db.getSnapshot('system', 'test', null, function(err, data) {
          assert.ok(err);
          assert.equal(data, null);
          done();
        });
      });
    });

    it('defaults to the version of the document if there are no ops', function(done) {
      var db = this.db;
      db.writeSnapshot('testcollection', 'versiontest', {type: 'json0', v: 3, data: {x: 5}}, function(err) {
        if (err) throw Error(err);
        db.getVersion('testcollection', 'versiontest', function(err, v) {
          if (err) throw Error(err);
          assert.equal(v, 3);
          done();
        });
      });
    });

    describe('query', function() {
      it('returns data in the collection', function(done) {
        var snapshot = {type: 'json0', v: 5, data: {x: 5, y: 6}};
        var db = this.db;
        db.writeSnapshot('testcollection', 'test', snapshot, function(err) {
          db.query('testcollection', {x: 5}, null, null, function(err, results) {
            if (err) throw Error(err);
            delete results[0].docName;
            assert.deepEqual(results, [snapshot]);
            done();
          });
        });
      });

      it('returns nothing when there is no data', function(done) {
        this.db.query('testcollection', {x: 5}, null, null, function(err, results) {
          if (err) throw Error(err);
          assert.deepEqual(results, []);
          done();
        });
      });

      it('does not allow $where queries', function(done) {
        this.db.query('testcollection', {$where: "true"}, null, null, function(err, results) {
          assert.ok(err);
          assert.equal(results, null);
          done();
        });
      });

      it('$distinct should perform distinct operation', function(done) {
        var snapshots = [
          {type: 'json0', v: 5, m: {}, data: {x: 1, y: 1}},
          {type: 'json0', v: 5, m: {}, data: {x: 2, y: 2}},
          {type: 'json0', v: 5, m: {}, data: {x: 3, y: 2}}
        ];
        var db = this.db;
        db.writeSnapshot('testcollection', 'test1', snapshots[0], function(err) {
          db.writeSnapshot('testcollection', 'test2', snapshots[1], function(err) {
            db.writeSnapshot('testcollection', 'test3', snapshots[2], function(err) {
              var query = {$distinct: true, $field: 'y', $query: {}};
              db.query('testcollection', query, null, null, function(err, results, extra) {
                if (err) throw Error(err);
                assert.deepEqual(extra, [1, 2]);
                done();
              });
            });
          });
        });
      });

      it('$aggregate should perform aggregate command', function(done) {
        var snapshots = [
          {type: 'json0', v: 5, m: {}, data: {x: 1, y: 1}},
          {type: 'json0', v: 5, m: {}, data: {x: 2, y: 2}},
          {type: 'json0', v: 5, m: {}, data: {x: 3, y: 2}}
        ];
        var db = this.db;
        db.allowAggregateQueries = true;
        db.writeSnapshot('testcollection', 'test1', snapshots[0], function(err) {
          db.writeSnapshot('testcollection', 'test2', snapshots[1], function(err) {
            db.writeSnapshot('testcollection', 'test3', snapshots[2], function(err) {
              var query = {$aggregate: [
                {$group: {_id: '$y', count: {$sum: 1}}},
                {$sort: {count: 1}}
              ]};
              db.query('testcollection', query, null, null, function(err, results, extra) {
                if (err) throw Error(err);
                assert.deepEqual(extra, [{_id: 1, count: 1}, {_id: 2, count: 2}]);
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
          assert.ok(err);
          done();
        });
      });

      it('does not allow $mapReduce queries by default', function(done) {
        var snapshots = [
          {type: 'json0', v: 5, m: {}, data: {player: 'a', round: 1, score: 5}},
          {type: 'json0', v: 5, m: {}, data: {player: 'a', round: 2, score: 7}},
          {type: 'json0', v: 5, m: {}, data: {player: 'b', round: 1, score: 15}}
        ];
        var db = this.db;
        db.writeSnapshot('testcollection', 'test1', snapshots[0], function(err) {
          db.writeSnapshot('testcollection', 'test2', snapshots[1], function(err) {
            db.writeSnapshot('testcollection', 'test3', snapshots[2], function(err) {
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
              db.query('testcollection', query, null, null, function(err, results) {
                assert.ok(err);
                assert.equal(results, null);
                done();
              });
            });
          });
        });
      });

      it('$mapReduce queries should work when allowJavaScriptQuery == true', function(done) {
        var snapshots = [
          {type: 'json0', v: 5, m: {}, data: {player: 'a', round: 1, score: 5}},
          {type: 'json0', v: 5, m: {}, data: {player: 'a', round: 2, score: 7}},
          {type: 'json0', v: 5, m: {}, data: {player: 'b', round: 1, score: 15}}
        ];
        var db = this.db;
        db.allowJSQueries = true;
        db.writeSnapshot('testcollection', 'test1', snapshots[0], function(err) {
          db.writeSnapshot('testcollection', 'test2', snapshots[1], function(err) {
            db.writeSnapshot('testcollection', 'test3', snapshots[2], function(err) {
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
                if (err) throw Error(err);
                assert.deepEqual(extra, [{_id: 'a', value: 12}, {_id: 'b', value: 15}]);
                done();
              });
            });
          });
        });
      });
    });

    describe('query with projection', function() {
      it('returns only projected fields', function(done) {
        var db = this.db;
        db.writeSnapshot('testcollection', 'test', {type: 'json0', v: 5, m: {}, data: {x: 5, y: 6}}, function(err) {
          db.query('testcollection', {x: 5}, {y: true}, null, function(err, results) {
            if (err) throw Error(err);
            assert.deepEqual(results, [{type: 'json0', v: 5, data: {y: 6}, docName: 'test'}]);
            done();
          });
        });
      });

      it('returns no data for matching documents if fields is empty', function(done) {
        var snapshot = {type: 'json0', v: 5, m: {}, data: {x: 5, y: 6}};
        var db = this.db;
        db.writeSnapshot('testcollection', 'test', snapshot, function(err) {
          db.query('testcollection', {x: 5}, {}, null, function(err, results) {
            if (err) throw Error(err);
            assert.deepEqual(results, [{type: 'json0', v: 5, data: {}, docName: 'test'}]);
            done();
          });
        });
      });
    });

    describe('queryPollDoc', function() {
      it('returns false when the document does not exist', function(done) {
        var db = this.db;
        db.queryPollDoc('testcollection', 'doesnotexist', {}, null, function(err, result) {
          if (err) throw Error(err);
          assert.equal(result, false);
          done();
        });
      });

      it('returns true when the document matches', function(done) {
        var snapshot = {type: 'json0', v: 5, m: {}, data: {x: 5, y: 6}};
        var db = this.db;
        db.writeSnapshot('testcollection', 'test', snapshot, function(err) {
          db.queryPollDoc('testcollection', 'test', {x: 5}, null, function(err, result) {
            if (err) throw Error(err);
            assert.equal(result, true);
            done();
          });
        });
      });

      it('returns false when the document does not match', function(done) {
        var snapshot = {type: 'json0', v: 5, m: {}, data: {x: 5, y: 6}};
        var db = this.db;
        db.writeSnapshot('testcollection', 'test', snapshot, function(err) {
          db.queryPollDoc('testcollection', 'test', {x: 6}, null, function(err, result) {
            if (err) throw Error(err);
            assert.equal(result, false);
            done();
          });
        });
      });

      it('does not allow $where queries', function(done) {
        this.db.queryPollDoc('testcollection', 'somedoc', {$where: "true"}, null, function(err) {
          assert.ok(err);
          done();
        });
      });
    });
  });

  require('livedb/test/snapshotdb')(create);
  require('livedb/test/oplog')(create);
});
