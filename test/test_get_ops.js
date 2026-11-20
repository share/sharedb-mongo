var expect = require('chai').expect;
var ShareDbMongo = require('..');
var sinon = require('sinon');

var mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';

function create(options, callback) {
  var opts = Object.assign({
    mongoOptions: {},
    getOpsWithoutStrictLinking: true
  }, options);
  var db = new ShareDbMongo(mongoUrl, opts);
  db.getDbs(function(err, mongo) {
    if (err) return callback(err);
    mongo.dropDatabase()
      .then(function() {
        callback(null, db, mongo);
      })
      .catch(callback);
  });
};

// loop thru strict linking options
[true, false].forEach(function(strictLinkingOption) {
  describe('getOps with strict linking ' + strictLinkingOption, function() {
    beforeEach(function(done) {
      var self = this;
      create(
        {getOpsWithoutStrictLinking: strictLinkingOption},
        function(err, db, mongo) {
          if (err) return done(err);
          self.db = db;
          self.mongo = mongo;
          done();
        });
    });

    afterEach(function(done) {
      this.db.close(done);
    });

    describe('a chain of ops', function() {
      var db;
      var mongo;
      var id;
      var collection;

      beforeEach(function(done) {
        db = this.db;
        mongo = this.mongo;
        id = 'document1';
        collection = 'testcollection';

        sinon.spy(db, '_getOps');
        sinon.spy(db, '_getSnapshotOpLink');

        var ops = [
          {v: 0, create: {}},
          {v: 1, p: ['foo'], oi: 'bar'},
          {v: 2, p: ['foo'], oi: 'baz'},
          {v: 3, p: ['foo'], oi: 'qux'}
        ];

        commitOpChain(db, mongo, collection, id, ops, function(error) {
          if (error) done(error);
          mongo.collection('o_' + collection).deleteOne({v: 1}).then(function() {
            done();
          });
        });
      });

      it('fetches ops 2-3 without fetching all ops', function(done) {
        db.getOps(collection, id, 2, 4, null, function(error, ops) {
          if (error) return done(error);
          expect(ops.length).to.equal(2);
          expect(ops[0].v).to.equal(2);
          expect(ops[1].v).to.equal(3);
          done();
        });
      });

      it('default option errors when missing ops', function(done) {
        db.getOps(collection, id, 0, 4, null, function(error) {
          expect(error.code).to.equal(5103);
          expect(error.message).to.equal('Missing ops from requested version testcollection.document1 0');
          done();
        });
      });

      it('ignoreMissingOps option returns ops up to the first missing op', function(done) {
        db.getOps(collection, id, 0, 4, {ignoreMissingOps: true}, function(error, ops) {
          if (error) return done(error);
          expect(ops.length).to.equal(2);
          expect(ops[0].v).to.equal(2);
          expect(ops[1].v).to.equal(3);
          done();
        });
      });

      it('getOpsToSnapshot ignoreMissingOps option returns ops up to the first missing op', function(done) {
        db.getSnapshot(collection, id, {$submit: true}, null, function(error, snapshot) {
          if (error) done(error);
          db.getOpsToSnapshot(collection, id, 0, snapshot, {ignoreMissingOps: true}, function(error, ops) {
            if (error) return done(error);
            expect(ops.length).to.equal(2);
            expect(ops[0].v).to.equal(2);
            expect(ops[1].v).to.equal(3);
            done();
          });
        });
      });
    });

    describe('concurrent requests', function() {
      var db;
      var mongo;
      var id = 'document1';
      var collection = 'testcollection';

      beforeEach(function(done) {
        db = this.db;
        mongo = this.mongo;
        var ops = [
          {v: 0, create: {}},
          {v: 1, p: ['foo'], oi: 'bar'},
          {v: 2, p: ['foo'], oi: 'baz'}
        ];
        commitOpChain(db, mongo, collection, id, ops, done);
      });

      function holdFetches() {
        var heldFetches = [];
        var getOps = db._getOps;
        sinon.stub(db, '_getOps').callsFake(function() {
          var args = arguments;
          heldFetches.push(function() {
            getOps.apply(db, args);
          });
        });
        return function releaseFetches() {
          heldFetches.splice(0).forEach(function(fetch) {
            fetch();
          });
        };
      }

      function onGetOpsRequest(callback) {
        var requests = db._getOpsRequests;
        var run = requests.run;
        var count = 0;
        sinon.stub(requests, 'run').callsFake(function() {
          run.apply(requests, arguments);
          callback(++count);
        });
      }

      function commitNextOp(op, callback) {
        mongo.collection('o_' + collection).find({d: id, v: op.v - 1}).next().then(function(previousOp) {
          commitOpChain(db, mongo, collection, id, [op], previousOp._id, op.v, callback);
        }, callback);
      }

      function collectResults(count, done, assertions) {
        var results = [];
        return function(error, ops) {
          if (error) return done(error);
          results.push(ops);
          if (results.length < count) return;
          try {
            assertions(results);
          } catch (assertionError) {
            return done(assertionError);
          }
          done();
        };
      }

      function versions(ops) {
        return ops.map(function(op) {
          return op.v;
        });
      }

      it('shares one fetch between identical requests', function(done) {
        var releaseFetches = holdFetches();
        onGetOpsRequest(function(requestNumber) {
          if (requestNumber === 2) releaseFetches();
        });
        var callback = collectResults(2, done, function(results) {
          expect(db._getOps.callCount).to.equal(1);
          expect(versions(results[0])).to.eql([0, 1, 2]);
          expect(results[1]).to.eql(results[0]);
          expect(results[1]).not.to.equal(results[0]);
        });

        db.getOps(collection, id, 0, null, null, callback);
        db.getOps(collection, id, 0, null, null, callback);
      });

      it('shares one fetch between requests with different agentCustom', function(done) {
        var releaseFetches = holdFetches();
        onGetOpsRequest(function(requestNumber) {
          if (requestNumber === 2) releaseFetches();
        });
        var lookupUserIds = [];
        db.use('beforeSnapshotLookup', function(request, next) {
          lookupUserIds.push(request.options.agentCustom.userId);
          next();
        });
        var callback = collectResults(2, done, function(results) {
          expect(db._getOps.callCount).to.equal(1);
          expect(results[1]).to.eql(results[0]);
          expect(lookupUserIds).to.have.members(['a', 'b']);
        });

        db.getOps(collection, id, 0, null, {agentCustom: {userId: 'a'}}, callback);
        db.getOps(collection, id, 0, null, {agentCustom: {userId: 'b', notSerializable: function() {}}}, callback);
      });

      it('does not share a fetch between requests with different metadata options', function(done) {
        var releaseFetches = holdFetches();
        onGetOpsRequest(function(requestNumber) {
          if (requestNumber === 2) releaseFetches();
        });
        var callback = collectResults(2, done, function() {
          expect(db._getOps.callCount).to.equal(2);
        });

        db.getOps(collection, id, 0, null, {metadata: true}, callback);
        db.getOps(collection, id, 0, null, null, callback);
      });

      it('does not share a fetch between requests with different ignoreMissingOps options', function(done) {
        var releaseFetches = holdFetches();
        onGetOpsRequest(function(requestNumber) {
          if (requestNumber === 2) releaseFetches();
        });
        var strictError;
        var lenientOps;
        var remaining = 2;
        function finish() {
          if (--remaining) return;
          try {
            expect(db._getOps.callCount).to.equal(2);
            expect(strictError.code).to.equal(5103);
            expect(versions(lenientOps)).to.eql([2]);
          } catch (assertionError) {
            return done(assertionError);
          }
          done();
        }

        mongo.collection('o_' + collection).deleteOne({v: 1}).then(function() {
          db.getOps(collection, id, 0, null, null, function(error) {
            strictError = error;
            finish();
          });
          db.getOps(collection, id, 0, null, {ignoreMissingOps: true}, function(error, ops) {
            if (error) return done(error);
            lenientOps = ops;
            finish();
          });
        }, done);
      });

      it('does not share a fetch with a request that started before a newer commit', function(done) {
        var releaseFetches = holdFetches();
        var firstOps;
        var secondOps;
        var callback = collectResults(2, done, function() {
          expect(db._getOps.callCount).to.equal(2);
          expect(versions(firstOps)).to.eql([0, 1, 2]);
          expect(versions(secondOps)).to.eql([0, 1, 2, 3]);
        });
        onGetOpsRequest(function(requestNumber) {
          if (requestNumber === 2) return releaseFetches();
          commitNextOp({v: 3, p: ['foo'], oi: 'qux'}, function(error) {
            if (error) return done(error);
            db.getOps(collection, id, 0, null, null, function(error, ops) {
              secondOps = ops;
              callback(error, ops);
            });
          });
        });

        db.getOps(collection, id, 0, null, null, function(error, ops) {
          firstOps = ops;
          callback(error, ops);
        });
      });

      it('shares a fetch for an explicit to across a newer commit only without strict linking', function(done) {
        var releaseFetches = holdFetches();
        var callback = collectResults(2, done, function(results) {
          expect(db._getOps.callCount).to.equal(db.getOpsWithoutStrictLinking ? 1 : 2);
          expect(versions(results[0])).to.eql([0, 1]);
          expect(versions(results[1])).to.eql([0, 1]);
        });
        onGetOpsRequest(function(requestNumber) {
          if (requestNumber === 2) return releaseFetches();
          commitNextOp({v: 3, p: ['foo'], oi: 'qux'}, function(error) {
            if (error) return done(error);
            db.getOps(collection, id, 0, 2, null, callback);
          });
        });

        db.getOps(collection, id, 0, 2, null, callback);
      });
    });
  });
});

function commitOpChain(db, mongo, collection, id, ops, previousOpId, version, callback) {
  if (typeof previousOpId === 'function') {
    callback = previousOpId;
    previousOpId = undefined;
    version = 0;
  }

  ops = ops.slice();
  var op = ops.shift();

  if (!op) {
    return callback();
  }

  var snapshot = {id: id, v: version + 1, type: 'json0', data: {}, m: null, _opLink: previousOpId};
  db.commit(collection, id, op, snapshot, null, function(error) {
    if (error) return callback(error);
    mongo.collection('o_' + collection).find({d: id, v: version}).next().then(function(op) {
      commitOpChain(db, mongo, collection, id, ops, (op ? op._id : null), ++version, callback);
    });
  });
}
