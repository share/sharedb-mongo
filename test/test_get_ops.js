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
    mongo.dropDatabase(function(err) {
      if (err) return callback(err);
      callback(null, db, mongo);
    });
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
          mongo.collection('o_' + collection).deleteOne({v: 1}, done);
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
    mongo.collection('o_' + collection).find({d: id, v: version}).next(function(error, op) {
      if (error) return callback(error);
      commitOpChain(db, mongo, collection, id, ops, (op ? op._id : null), ++version, callback);
    });
  });
}
