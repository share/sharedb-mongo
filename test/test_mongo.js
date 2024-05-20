var expect = require('chai').expect;
var ShareDbMongo = require('..');
var getQuery = require('sharedb-mingo-memory/get-query');
var async = require('async');
var randomUUID = require('crypto').randomUUID;

var mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';

function create(callback) {
  var db = new ShareDbMongo(mongoUrl);
  db.getDbs(function(err, mongo) {
    if (err) return callback(err);
    mongo.dropDatabase()
      .then(function() {
        callback(null, db, mongo);
      })
      .catch(callback);
  });
};

require('sharedb/test/db')({create: create, getQuery: getQuery});

describe('mongo db', function() {
  beforeEach(function(done) {
    var self = this;
    create(function(err, db, mongo) {
      if (err) return done(err);
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
        if (err) return done(err);
        mongo.collection('o_testcollection').indexInformation().then(function(indexes) {
          // Index for getting document(s) ops
          expect(indexes['d_1_v_1']).ok;
          // Index for checking committed op(s) by src and seq
          expect(indexes['src_1_seq_1_v_1']).ok;
          done();
        });
      });
    });

    it('respects unique indexes', function(done) {
      var db = this.db;
      this.mongo.collection('testcollection').createIndex({x: 1}, {unique: true}).then(function() {
        db.commit('testcollection', 'foo', {v: 0, create: {}}, {v: 1, data: {x: 7}}, null, function(err) {
          if (err) return done(err);
          db.commit('testcollection', 'bar', {v: 0, create: {}}, {v: 1, data: {x: 7}}, null, function(err) {
            expect(err && err.code).equal(11000);
            done();
          });
        });
      });
    });
  });

  describe('security options', function() {
    it('does not allow editing the system collection', function(done) {
      var db = this.db;
      db.commit('system', 'test', {v: 0, create: {}}, {}, null, function(err) {
        expect(err).ok;
        db.getSnapshot('system', 'test', null, null, function(err) {
          expect(err).ok;
          done();
        });
      });
    });
  });

  describe('query', function() {
    it('$count should count documents', function(done) {
      var snapshots = [
        {type: 'json0', id: 'test1', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', id: 'test2', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', id: 'test3', v: 1, data: {x: 3, y: 2}}
      ];
      var query = {$count: true, y: 2};

      var db = this.db;
      async.each(snapshots, function(snapshot, cb) {
        db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, cb);
      }, function(err) {
        if (err) return done(err);
        db.query('testcollection', query, null, null, function(err, results, extra) {
          if (err) return done(err);

          expect(results).eql([]);
          expect(extra).eql(2);
          done();
        });
      });
    });

    it('$map maps docs with output in extra', function(done) {
      var snapshots = [
        {type: 'json0', id: 'test1', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', id: 'test2', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', id: 'test3', v: 1, data: {x: 3, y: 2}}
      ];
      var query = {
        y: 2,
        $sort: {x: 1},
        $map: function(doc) {
          return doc.x;
        }
      };

      var db = this.db;
      async.each(snapshots, function(snapshot, cb) {
        db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, cb);
      }, function(err) {
        if (err) return done(err);
        db.query('testcollection', query, null, null, function(err, results, extra) {
          if (err) return done(err);

          // Since $map can return non-docs, the output is delivered in extra.
          expect(results).eql([]);
          expect(extra).to.deep.equal([2, 3]);
          done();
        });
      });
    });

    it('$explain delivers output in extra', function(done) {
      var snapshots = [
        {type: 'json0', id: 'test1', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', id: 'test2', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', id: 'test3', v: 1, data: {x: 3, y: 2}}
      ];
      var query = {$explain: true, y: 2};

      var db = this.db;
      async.each(snapshots, function(snapshot, cb) {
        db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, cb);
      }, function(err) {
        if (err) return done(err);
        db.query('testcollection', query, null, null, function(err, results, extra) {
          if (err) return done(err);

          expect(results).eql([]);
          // Just check for the presence of an explain result. The specific structure
          // could vary between Mongo versions.
          expect(extra).to.be.an('object');
          done();
        });
      });
    });

    it('$sort, $skip and $limit should order, skip and limit', function(done) {
      var snapshots = [
        {type: 'json0', v: 1, data: {x: 1}, id: 'test1', m: null},
        {type: 'json0', v: 1, data: {x: 3}, id: 'test2', m: null}, // intentionally added out of sort order
        {type: 'json0', v: 1, data: {x: 2}, id: 'test3', m: null}
      ];
      var query = {$sort: {x: 1}, $skip: 1, $limit: 1};

      var db = this.db;
      async.each(snapshots, function(snapshot, cb) {
        db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, cb);
      }, function(err) {
        if (err) return done(err);

        db.query('testcollection', query, null, null, function(err, results) {
          if (err) throw err;
          expect(results).eql([snapshots[2]]);
          done();
        });
      });
    });

    it('does not allow $where queries', function(done) {
      this.db.query('testcollection', {$where: 'true'}, null, null, function(err) {
        expect(err).ok;
        done();
      });
    });

    it('queryPollDoc does not allow $where queries', function(done) {
      this.db.queryPollDoc('testcollection', 'somedoc', {$where: 'true'}, null, function(err) {
        expect(err).ok;
        done();
      });
    });

    it('$query is deprecated', function(done) {
      this.db.query('testcollection', {$query: {}}, null, null, function(err) {
        expect(err).ok;
        expect(err.code).eql(4106);
        done();
      });
    });

    it('only one collection operation allowed', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $aggregate: {}}, null, null, function(err) {
        expect(err).ok;
        expect(err.code).eql(4108);
        done();
      });
    });

    it('only one cursor operation allowed', function(done) {
      this.db.query('testcollection', {$count: true, $explain: true}, null, null, function(err) {
        expect(err).ok;
        expect(err.code).eql(4109);
        done();
      });
    });

    it('cursor transform can\'t run after collection operation', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $sort: {y: 1}}, null, null, function(err) {
        expect(err).ok;
        expect(err.code).eql(4110);
        done();
      });
    });

    it('cursor operation can\'t run after collection operation', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $count: true}, null, null, function(err) {
        expect(err).ok;
        expect(err.code).eql(4110);
        done();
      });
    });

    it('non-object $readPref should return error', function(done) {
      this.db.query('testcollection', {$readPref: true}, null, null, function(err) {
        expect(err).ok;
        expect(err.code).eql(4107);
        done();
      });
    });

    it('malformed $mapReduce should return error', function(done) {
      this.db.allowJSQueries = true; // required for $mapReduce
      this.db.query('testcollection', {$mapReduce: true}, null, null, function(err) {
        expect(err).ok;
        expect(err.code).eql(4107);
        done();
      });
    });

    describe('queryPollDoc correctly filters on _id', function() {
      var snapshot = {type: 'json0', v: 1, data: {}, id: 'test'};

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
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            if (err) return done(err);
            var query = {$distinct: {field: 'y'}};
            db.query('testcollection', query, null, null, function(err, results, extra) {
              if (err) return done(err);
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
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            if (err) return done(err);
            var query = {$aggregate: [
              {$group: {_id: '$y', count: {$sum: 1}}},
              {$sort: {count: 1}}
            ]};
            db.query('testcollection', query, null, null, function(err, results, extra) {
              if (err) return done(err);
              expect(extra).eql([{_id: 1, count: 1}, {_id: 2, count: 2}]);
              done();
            });
          });
        });
      });
    });

    it('does not let you run $aggregate queries without options.allowAggregateQueries', function(done) {
      var query = {$aggregate: [
        {$group: {_id: '$y', count: {$sum: 1}}},
        {$sort: {count: 1}}
      ]};
      this.db.query('testcollection', query, null, null, function(err) {
        expect(err).ok;
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
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            if (err) return done(err);
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
              expect(err).ok;
              done();
            });
          });
        });
      });
    });

    it('$mapReduce queries should work when allowJavaScriptQuery == true', function(done) {
      // This function was removed in mongodb5:
      // https://github.com/mongodb/node-mongodb-native/pull/3511
      var mapReduceSupported = typeof this.db.mongo.collection('test').mapReduce === 'function';
      if (!mapReduceSupported) {
        return done();
      }
      var snapshots = [
        {type: 'json0', v: 1, data: {player: 'a', round: 1, score: 5}},
        {type: 'json0', v: 1, data: {player: 'a', round: 2, score: 7}},
        {type: 'json0', v: 1, data: {player: 'b', round: 1, score: 15}}
      ];
      var db = this.db;
      db.allowJSQueries = true;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, function(err) {
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, function(err) {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, function(err) {
            if (err) return done(err);
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
              if (err) return done(err);
              expect(extra).to.have.deep.members([{_id: 'a', value: 12}, {_id: 'b', value: 15}]);
              done();
            });
          });
        });
      });
    });
  });
});

describe('options', function() {
  var db;

  afterEach(function(done) {
    db.close(done);
  });

  it('disables index creation', function(done) {
    var collection = randomUUID();
    db = new ShareDbMongo(mongoUrl, {disableIndexCreation: true});
    async.waterfall([
      db.commit.bind(db, collection, 'foo', {v: 0, create: {}}, {}, null),
      function(succeeded, next) {
        db.getDbs(next);
      },
      function(mongo, mongoPoll, next) {
        mongo.collection('o_' + collection).indexInformation().then(function(indexes) {
          expect(indexes['d_1_v_1']).not.to.be.ok;
          expect(indexes['src_1_seq_1_v_1']).not.to.be.ok;
          next();
        });
      }
    ], done);
  });

  it('disables just the src,seq,v index', function(done) {
    var collection = randomUUID();
    db = new ShareDbMongo(mongoUrl, {disableIndexCreation: {src_seq_v: true}});
    async.waterfall([
      db.commit.bind(db, collection, 'foo', {v: 0, create: {}}, {}, null),
      function(succeeded, next) {
        db.getDbs(next);
      },
      function(mongo, mongoPoll, next) {
        mongo.collection('o_' + collection).indexInformation().then(function(indexes) {
          expect(indexes['d_1_v_1']).to.be.ok;
          expect(indexes['src_1_seq_1_v_1']).not.to.be.ok;
          next();
        });
      }
    ], done);
  });
});

describe('mongo db connection', function() {
  describe('via url string', function() {
    beforeEach(function(done) {
      this.db = new ShareDbMongo({mongo: mongoUrl});

      // This will enqueue the callback, testing the 'pendingConnect'
      // logic.
      this.db.getDbs(function(err, mongo) {
        if (err) return done(err);
        mongo.dropDatabase()
          .then(function() {
            done();
          })
          .catch(done);
      });
    });

    afterEach(function(done) {
      this.db.close(done);
    });

    it('commit and query', function(done) {
      var snapshot = {type: 'json0', v: 1, data: {}, id: 'test', m: null};
      var db = this.db;

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        db.query('testcollection', {}, null, null, function(err, results) {
          if (err) return done(err);
          expect(results).eql([snapshot]);
          done();
        });
      });
    });
  });

  describe('via url string with mongoPoll and pollDelay option', function() {
    beforeEach(function(done) {
      this.pollDelay = 1000;
      this.db = new ShareDbMongo({mongo: mongoUrl, mongoPoll: mongoUrl, pollDelay: this.pollDelay});
      done();
    });

    afterEach(function(done) {
      this.db.close(done);
    });

    it('delays queryPoll but not commit', function(done) {
      var db = this.db;
      var pollDelay = this.pollDelay;

      var snapshot = {type: 'json0', v: 1, data: {}, id: 'test'};
      var timeBeforeCommit = new Date;
      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        expect((new Date) - timeBeforeCommit).lessThan(pollDelay);

        var timeBeforeQuery = new Date;
        db.queryPoll('testcollection', {}, null, function(err, results) {
          if (err) return done(err);
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

  var addsType = function(query) {
    var queryWithTypeNeNull = shallowClone(query);
    queryWithTypeNeNull._type = {$type: 2};
    var parsedQuery = parseQuery(query);
    makeQuerySafe(parsedQuery.query);
    expect(parsedQuery.query).eql(queryWithTypeNeNull);
  };

  var doesNotModify = function(query) {
    var parsedQuery = parseQuery(query);
    makeQuerySafe(parsedQuery.query);
    expect(parsedQuery.query).eql(query);
  };

  describe('adds _type: {$type: 2} when necessary', function() {
    it('basic', function() {
      addsType({});
      addsType({foo: null});
      doesNotModify({foo: 1});
      addsType({foo: {$bitsAllSet: 1}}); // We don't try to analyze $bitsAllSet
    });

    it('does not modify already set type', function() {
      doesNotModify({_type: null});
      doesNotModify({_type: 'foo'});
      doesNotModify({_type: {$ne: null}});
    });

    it('ignores fields that remain set on deleted docs', function() {
      addsType({_id: 'x'});
      addsType({_o: 'x'});
      addsType({_v: 2});
      addsType({_m: {mtime: 2}});
      addsType({'_m.mtime': 2});

      addsType({_id: 'x', foo: null});
      addsType({_o: 'x', foo: null});
      addsType({_v: 2, foo: null});
      addsType({_m: {mtime: 2}, foo: null});
      addsType({'_m.mtime': 2, foo: null});

      doesNotModify({_id: 'x', foo: 1});
      doesNotModify({_o: 'x', foo: 1});
      doesNotModify({_v: 2, foo: 1});
      doesNotModify({_m: {mtime: 2}, foo: 1});
      doesNotModify({'_m.mtime': 2, foo: 1});
    });

    it('$ne', function() {
      addsType({foo: {$ne: 1}});
      doesNotModify({foo: {$ne: 1}, bar: 1});
      doesNotModify({foo: {$ne: null}});
    });

    it('comparisons', function() {
      doesNotModify({foo: {$gt: 1}});
      doesNotModify({foo: {$gte: 1}});
      doesNotModify({foo: {$lt: 1}});
      doesNotModify({foo: {$lte: 1}});
      doesNotModify({foo: {$gte: 2, $lte: 5}});
      addsType({foo: {$gte: null, $lte: null}});
    });

    it('$exists', function() {
      doesNotModify({foo: {$exists: true}});
      addsType({foo: {$exists: false}});
      doesNotModify({foo: {$exists: true}, bar: {$exists: false}});
    });

    it('$not', function() {
      addsType({$not: {foo: 1}});
      addsType({$not: {foo: null}}); // We don't try to analyze $not
    });

    it('$in', function() {
      doesNotModify({foo: {$in: [1, 2, 3]}});
      addsType({foo: {$in: [null, 2, 3]}});
      doesNotModify({foo: {$in: [null, 2, 3]}, bar: 1});
    });

    it('top-level $and alone', function() {
      doesNotModify({$and: [{foo: {$ne: null}}, {bar: {$ne: null}}]});
      doesNotModify({$and: [{foo: {$ne: 1}}, {bar: {$ne: null}}]});
      addsType({$and: [{foo: {$ne: 1}}, {bar: {$ne: 1}}]});
    });

    describe('top-level $and with other conditions', function() {
      // If the top-level $and could match a deleted doc, it should continue looking at the other
      // top-level conditions.
      it('does not add _type for "field: nonNullValue"', function() {
        doesNotModify({$and: [{foo: {$ne: true}}], bar: 1});
      });
    });

    it('top-level $or alone', function() {
      doesNotModify({$or: [{foo: {$ne: null}}, {bar: {$ne: null}}]});
      addsType({$or: [{foo: {$ne: 1}}, {bar: {$ne: null}}]});
      addsType({$or: [{foo: {$ne: 1}}, {bar: {$ne: 1}}]});
    });

    describe('top-level $or with other conditions', function() {
      // With conditions at top level that don't match deleted docs, the $or doesn't matter.
      it('does not add _type for "field: nonNullValue"', function() {
        doesNotModify({$or: [{foo: {$ne: true}}], bar: 1});
      });
      it('does not add _type for $in with nonNullValues', function() {
        doesNotModify({$or: [{foo: {$ne: true}}], bar: {$in: ['a', 'b']}});
      });
      // If the other conditions could match deleted docs, then the $or _does_ matter.
      it('adds _type for "field: null"', function() {
        addsType({$or: [{foo: {$ne: true}}], bar: null});
      });
      it('adds _type for $in with a null value', function() {
        addsType({$or: [{foo: {$ne: true}}], bar: {$in: ['a', 'b', null]}});
      });
      it('does not add _type when all $or branches do not match deleted docs', function() {
        doesNotModify({
          $or: [
            {foo: {$ne: null}},
            {bar: 1}
          ],
          baz: null
        });
      });
    });

    it('malformed queries', function() {
      // if we don't understand the query, definitely don't mark it as
      // "safe as is"
      addsType({$or: {foo: 3}});
      addsType({foo: {$or: 3}});
      addsType({$not: [1, 2]});
      addsType({foo: {$bad: 1}});
      addsType({$bad: [2, 3]});
      addsType({$and: [[{foo: 1}]]});
      addsType({$and: 1});
      addsType({$expr: {$gt: {$subtract: ['$endDate', '$startDate']}}});
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
