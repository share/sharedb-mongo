var expect = require('expect.js');
var mongodb = require('mongodb');
var ShareDbMongo = require('..');
var getQuery = require('sharedb-mingo-memory/get-query');
var sinon = require('sinon');

var mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';

function create(callback) {
  var db = ShareDbMongo({
    mongo: function(shareDbCallback) {
      mongodb.connect(mongoUrl, function(err, mongo) {
        if (err) return callback(err);
        mongo.dropDatabase(function(err) {
          if (err) return callback(err);
          shareDbCallback(null, mongo);
          callback(null, db, mongo);
        });
      });
    },
    getOpsWithoutStrictLinking: true,
  });
};

require('sharedb/test/db')({create: create, getQuery: getQuery});

describe('getOpsWithoutStrictLinking: true', function () {
  beforeEach(function (done) {
    var self = this;
    create(function (err, db, mongo) {
      if (err) return done(err);
      self.db = db;
      self.mongo = mongo;
      done();
    });
  });

  afterEach(function (done) {
    this.db.close(done);
  });

  describe('a chain of ops', function () {
    var db
    var mongo;
    var id;
    var collection;

    beforeEach(function (done) {
      db = this.db;
      mongo = this.mongo;
      id = 'document1';
      collection = 'testcollection';

      sinon.spy(db, '_getOps');
      sinon.spy(db, '_getSnapshotOpLink');

      var ops = [
        { v: 0, create: {} },
        { v: 1, p: ['foo'], oi: 'bar' },
        { v: 2, p: ['foo'], oi: 'baz' },
        { v: 3, p: ['foo'], oi: 'qux' }
      ];

      commitOpChain(db, mongo, collection, id, ops, done);
    });

    it('fetches ops 0-1 without fetching all ops', function (done) {
      db.getOps(collection, id, 0, 2, null, function (error, ops) {
        if (error) return done(error);
        expect(ops.length).to.be(2);
        expect(ops[0].v).to.be(0);
        expect(ops[1].v).to.be(1);
        expect(db._getSnapshotOpLink.notCalled).to.be(true);
        expect(db._getOps.calledOnceWith(collection, id, 0, 2)).to.be(true);
        done();
      });
    });

    it('fetches ops 0-1 when v1 has a spurious duplicate', function (done) {
      var spuriousOp = { v: 1, d: id, p: ['foo'], oi: 'corrupt', o: null };

      callInSeries([
        function (next) {
          mongo.collection('o_' + collection).insert(spuriousOp, next);
        },
        function (result, next) {
          db.getOps(collection, id, 0, 2, null, next);
        },
        function (ops, next) {
          expect(ops.length).to.be(2);
          expect(ops[1].oi).to.be('bar');
          expect(db._getSnapshotOpLink.notCalled).to.be(true);
          expect(db._getOps.calledOnceWith(collection, id, 0, 2)).to.be(true);
          next();
        },
        done
      ]);
    });

    it('fetches ops 0-1 when the next op v2 has a spurious duplicate', function (done) {
      var spuriousOp = { v: 2, d: id, p: ['foo'], oi: 'corrupt', o: null };

      callInSeries([
        function (next) {
          mongo.collection('o_' + collection).insert(spuriousOp, next);
        },
        function (result, next) {
          db.getOps(collection, id, 0, 2, null, next);
        },
        function (ops, next) {
          expect(ops.length).to.be(2);
          expect(ops[1].oi).to.be('bar');
          expect(db._getSnapshotOpLink.notCalled).to.be(true);
          expect(db._getOps.calledOnceWith(collection, id, 0, 3)).to.be(true);
          next();
        },
        done
      ]);
    });

    it('fetches ops 0-1 when all the ops have spurious duplicates', function (done) {
      var spuriousOps = [
        { v: 0, d: id, p: ['foo'], oi: 'corrupt', o: null },
        { v: 1, d: id, p: ['foo'], oi: 'corrupt', o: null },
        { v: 2, d: id, p: ['foo'], oi: 'corrupt', o: null },
        { v: 3, d: id, p: ['foo'], oi: 'corrupt', o: null },
      ];

      callInSeries([
        function (next) {
          mongo.collection('o_' + collection).insertMany(spuriousOps, next);
        },
        function (result, next) {
          db.getOps(collection, id, 0, 2, null, next);
        },
        function (ops, next) {
          expect(ops.length).to.be(2);
          expect(ops[0].create).to.eql({});
          expect(ops[1].oi).to.be('bar');
          expect(db._getSnapshotOpLink.calledOnce).to.be(true);
          next();
        },
        done,
      ])
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

  var snapshot = { id: id, v: version + 1, type: 'json0', data: {}, m: null, _opLink: previousOpId };
  db.commit(collection, id, op, snapshot, null, function (error) {
    if (error) return callback(error);
    mongo.collection('o_' + collection).find({ d: id, v: version }).next(function (error, op) {
      if (error) return callback(error);
      commitOpChain(db, mongo, collection, id, ops, op._id, ++version, callback);
    });
  });
}

function callInSeries(callbacks, args) {
  if (!callbacks.length) return;
  args = args || [];
  var error = args.shift();

  if (error) {
    var finalCallback = callbacks[callbacks.length - 1];
    return finalCallback(error);
  }

  var callback = callbacks.shift();
  if (callbacks.length) {
    args.push(function () {
      var args = Array.from(arguments);
      callInSeries(callbacks, args);
    });
  }

  callback.apply(callback, args);
}
