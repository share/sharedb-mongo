var async = require('async');
var expect = require('chai').expect;
var ShareDbMongo = require('..');

var mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';
var BEFORE_EDIT = ShareDbMongo.MiddlewareActions.beforeOverwrite;
var BEFORE_CREATE = ShareDbMongo.MiddlewareActions.beforeCreate;
var BEFORE_SNAPSHOT_LOOKUP = ShareDbMongo.MiddlewareActions.beforeSnapshotLookup;

function create(callback) {
  var db = new ShareDbMongo(mongoUrl);
  db.getDbs(function(err, mongo) {
    if (err) return callback(err);
    mongo.dropDatabase(function(err) {
      if (err) return callback(err);
      callback(null, db, mongo);
    });
  });
};

describe('mongo db middleware', function() {
  var db;

  beforeEach(function(done) {
    create(function(err, createdDb) {
      if (err) return done(err);
      db = createdDb;
      done();
    });
  });

  afterEach(function(done) {
    db.close(done);
  });

  describe('error handling', function() {
    it('throws error when no action is given', function() {
      function invalidAction() {
        db.use(null, function(_request, next) {
          next();
        });
      }
      expect(invalidAction).to.throw();
    });

    it('throws error when no handler is given', function() {
      function invalidAction() {
        db.use('someAction');
      }
      expect(invalidAction).to.throw();
    });

    it('throws error on unrecognized action name', function() {
      function invalidAction() {
        db.use('someAction', function(_request, next) {
          next();
        });
      }
      expect(invalidAction).to.throw();
    });
  });

  describe(BEFORE_EDIT, function() {
    it('has the expected properties on the request object', function(done) {
      db.use(BEFORE_EDIT, function(request, next) {
        expect(request).to.have.all.keys([
          'action',
          'collectionName',
          'documentToWrite',
          'op',
          'options',
          'query'
        ]);
        expect(request.action).to.equal(BEFORE_EDIT);
        expect(request.collectionName).to.equal('testcollection');
        expect(request.documentToWrite.foo).to.equal('fuzz');
        expect(request.op.op).to.exist;
        expect(request.options.testOptions).to.equal('yes');
        expect(request.query._id).to.equal('test1');
        next();
        done();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};
      var editOp = {v: 2, op: [{p: ['foo'], oi: 'bar', oi: 'baz'}], m: {ts: Date.now()}};
      var newSnapshot = {type: 'json0', id: 'test1', v: 2, data: {foo: 'fuzz'}};

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        db.commit('testcollection', snapshot.id, editOp, newSnapshot, {testOptions: 'yes'}, function(err) {
          if (err) return done(err);
        });
      });
    });

    it('should augment query filter and write to the document when commit is called', function(done) {
      // Augment the query. The original query looks up the document by id, wheras this middleware
      // changes it to use the `foo` property. The end result still returns the same document. The next
      // middleware ensures we attached it to the request.
      // We can't truly change which document is returned from the query because MongoDB will not allow
      // the immutable fields such as `_id` to be changed.
      db.use(BEFORE_EDIT, function(request, next) {
        request.query.foo = 'bar';
        next();
      });
      // Attach this middleware to check that the original one is passing the context
      // correctly. Commit will be called after this.
      db.use(BEFORE_EDIT, function(request, next) {
        expect(request.query).to.deep.equal({
          _id: 'test1',
          _v: 1,
          foo: 'bar'
        });
        next();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};
      var editOp = {v: 2, op: [{p: ['foo'], oi: 'bar', oi: 'baz'}], m: {ts: Date.now()}};
      var newSnapshot = {type: 'json0', id: 'test1', v: 2, data: {foo: 'baz'}};

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        expectDocumentToContainFoo('bar', function() {
          db.commit('testcollection', snapshot.id, editOp, newSnapshot, null, function(err) {
            if (err) return done(err);
            // Ensure the value is updated as expected
            expectDocumentToContainFoo('baz', done);
          });
        });
      });
    });
  });

  it('should augment the written document when commit is called', function(done) {
    // Change the written value of foo to be `fuzz`
    db.use(BEFORE_EDIT, function(request, next) {
      request.documentToWrite.foo = 'fuzz';
      next();
    });

    var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};

    // Issue a commit to change `bar` to `baz`
    var editOp = {v: 2, op: [{p: ['foo'], oi: 'bar', oi: 'baz'}], m: {ts: Date.now()}};
    var newSnapshot = {type: 'json0', id: 'test1', v: 2, data: {foo: 'baz'}};

    db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
      if (err) return done(err);
      expectDocumentToContainFoo('bar', function() {
        db.commit('testcollection', snapshot.id, editOp, newSnapshot, null, function(err) {
          if (err) return done(err);
          // Ensure the value is updated as expected
          expectDocumentToContainFoo('fuzz', done);
        });
      });
    });
  });

  describe(BEFORE_CREATE, function() {
    it('has the expected properties on the request object', function(done) {
      db.use(BEFORE_CREATE, function(request, next) {
        expect(request).to.have.all.keys([
          'action',
          'collectionName',
          'documentToWrite',
          'op',
          'options'
        ]);
        expect(request.action).to.equal(BEFORE_CREATE);
        expect(request.collectionName).to.equal('testcollection');
        expect(request.documentToWrite.foo).to.equal('bar');
        expect(request.op).to.exist;
        expect(request.options.testOptions).to.equal('baz');
        next();
        done();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, {testOptions: 'baz'}, function(err) {
        if (err) return done(err);
      });
    });

    it('should augment the written document when commit is called', function(done) {
      // Change the written value of foo to be `fuzz`
      db.use(BEFORE_CREATE, function(request, next) {
        request.documentToWrite.foo = 'fuzz';
        next();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        expectDocumentToContainFoo('fuzz', done);
      });
    });

    it('returns without writing when there was a middleware error', function(done) {
      db.use(BEFORE_CREATE, function(_, next) {
        next(new Error('Oh no!'));
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        expectDocumentNotToExist(function() {
          if (err) return done();
        });
      });
    });
  });

  describe(BEFORE_SNAPSHOT_LOOKUP, function() {
    it('has the expected properties on the request object before getting a single snapshot', function(done) {
      db.use(BEFORE_SNAPSHOT_LOOKUP, function(request, next) {
        expect(request).to.have.all.keys([
          'action',
          'collectionName',
          'options',
          'query'
        ]);
        expect(request.action).to.equal(BEFORE_SNAPSHOT_LOOKUP);
        expect(request.collectionName).to.equal('testcollection');
        expect(request.options.testOptions).to.equal('yes');
        expect(request.query._id).to.equal('test1');
        next();
        done();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};
      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        db.getSnapshot('testcollection', 'test1', null, {testOptions: 'yes'}, function(err, doc) {
          if (err) return done(err);
          expect(doc).to.exist;
        });
      });
    });

    it('has the expected properties on the request object before getting bulk snapshots', function(done) {
      db.use(BEFORE_SNAPSHOT_LOOKUP, function(request, next) {
        expect(request).to.have.all.keys([
          'action',
          'collectionName',
          'options',
          'query'
        ]);
        expect(request.action).to.equal(BEFORE_SNAPSHOT_LOOKUP);
        expect(request.collectionName).to.equal('testcollection');
        expect(request.options.testOptions).to.equal('yes');
        expect(request.query._id).to.deep.equal({$in: ['test1']});
        next();
        done();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};
      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        db.getSnapshotBulk('testcollection', ['test1'], null, {testOptions: 'yes'}, function(err, doc) {
          if (err) return done(err);
          expect(doc).to.exist;
        });
      });
    });

    it('should augment the query when getSnapshot is called', function(done) {
      var snapshots = [
        {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}},
        {type: 'json0', id: 'test2', v: 1, data: {foo: 'baz'}}
      ];

      async.each(snapshots, function(snapshot, cb) {
        db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, cb);
      }, function(err) {
        if (err) return done(err);
        db.getSnapshot('testcollection', 'test1', null, null, function(err, doc) {
          if (err) return done(err);
          expect(doc.data).eql({
            foo: 'bar'
          });

          // Change the query to look for baz and not bar
          db.use(BEFORE_SNAPSHOT_LOOKUP, function(request, next) {
            request.query = {_id: 'test2'};
            next();
          });

          db.getSnapshot('testcollection', 'test1', null, null, function(err, doc) {
            if (err) return done(err);
            expect(doc.data).eql({
              foo: 'baz'
            });
            done();
          });
        });
      });
    });

    it('should augment the query when getSnapshotBulk is called', function(done) {
      var snapshots = [
        {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}},
        {type: 'json0', id: 'test2', v: 1, data: {foo: 'baz'}}
      ];

      async.each(snapshots, function(snapshot, cb) {
        db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, cb);
      }, function(err) {
        if (err) return done(err);
        db.getSnapshotBulk('testcollection', ['test1', 'test2'], null, null, function(err, docs) {
          if (err) return done(err);
          expect(docs.test1.data.foo).to.equal('bar');
          expect(docs.test2.data.foo).to.equal('baz');

          // Change the query to look for baz and not bar
          db.use(BEFORE_SNAPSHOT_LOOKUP, function(request, next) {
            request.query = {_id: {$in: ['test2']}};
            next();
          });

          db.getSnapshotBulk('testcollection', ['test1', 'test2'], null, null, function(err, docs) {
            if (err) return done(err);
            expect(docs.test1.data).not.to.exist;
            expect(docs.test2.data.foo).to.equal('baz');
            done();
          });
        });
      });
    });
  });

  function expectDocumentToContainFoo(valueOfFoo, cb) {
    var query = {_id: 'test1'};

    db.query('testcollection', query, null, null, function(err, results) {
      if (err) return done(err);
      expect(results[0].data).eql({
        foo: valueOfFoo
      });
      cb();
    });
  };

  function expectDocumentNotToExist(cb) {
    var query = {_id: 'test1'};

    db.query('testcollection', query, null, null, function(err, results) {
      if (err) return done(err);
      expect(results).to.be.empty;
      cb();
    });
  };
});
