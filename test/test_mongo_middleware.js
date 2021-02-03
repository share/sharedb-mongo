var async = require('async');
var expect = require('chai').expect;
var ShareDbMongo = require('..');

var mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';

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

  describe('beforeEdit', function() {
    it('has the expected properties on the request object', function(done) {
      var db = this.db;
      db.use('beforeEdit', function(request, next) {
        expect(request).to.have.all.keys([
          'action',
          'collectionName',
          'doc',
          'op',
          'options',
          'query'
        ]);
        expect(request.action).to.equal('beforeEdit');
        expect(request.collectionName).to.equal('testcollection');
        expect(request.doc.foo).to.equal('fuzz');
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
      var db = this.db;
      // Augment the query. The original query looks up the document by id, wheras this middleware
      // changes it to use the `foo` property. The end result still returns the same document. The next
      // middleware ensures we attached it to the request.
      // We can't truly change which document is returned from the query because MongoDB will not allow
      // the immutable fields such as `_id` to be changed.
      db.use('beforeEdit', function(request, next) {
        request.query.foo = 'bar';
        next();
      });
      // Attach this middleware to check that the original one is passing the context
      // correctly. Commit will be called after this.
      db.use('beforeEdit', function(request, next) {
        expect(request.query).to.deep.equal({
          _id: 'test1',
          _v: 1,
          foo: 'bar'
        });
        next();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};
      var editOp = {v: 2, op: [{p: ['foo'], oi: 'bar', oi: 'baz'}], m: {ts: Date.now()}};
      var newSnapshot = {type: 'json0', id: 'test1', v: 2, data: {foo: 'fuzz'}};

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        expectDocumentToContainFoo(db, 'bar', function() {
          db.commit('testcollection', snapshot.id, editOp, newSnapshot, null, function(err) {
            if (err) return done(err);
            // Ensure the value is updated as expected
            expectDocumentToContainFoo(db, 'fuzz', done);
          });
        });
      });
    });
  });

  it('should augment the written document when commit is called', function(done) {
    var db = this.db;
    // Change the written value of foo to be `fuzz`
    db.use('beforeEdit', function(request, next) {
      request.doc.foo = 'fuzz';
      next();
    });

    var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};

    // Issue a commit to change `bar` to `baz`
    var editOp = {v: 2, op: [{p: ['foo'], oi: 'bar', oi: 'baz'}], m: {ts: Date.now()}};
    var newSnapshot = {type: 'json0', id: 'test1', v: 2, data: {foo: 'fuzz'}};

    db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
      if (err) return done(err);
      expectDocumentToContainFoo(db, 'bar', function() {
        db.commit('testcollection', snapshot.id, editOp, newSnapshot, null, function(err) {
          if (err) return done(err);
          // Ensure the value is updated as expected
          expectDocumentToContainFoo(db, 'fuzz', done);
        });
      });
    });
  });

  describe('beforeSnapshotLookup', function() {
    it('has the expected properties on the request object before getting a single snapshot', function(done) {
      var db = this.db;
      db.use('beforeSnapshotLookup', function(request, next) {
        expect(request).to.have.all.keys([
          'action',
          'collectionName',
          'options',
          'query'
        ]);
        expect(request.action).to.equal('beforeSnapshotLookup');
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
      var db = this.db;
      db.use('beforeSnapshotLookup', function(request, next) {
        expect(request).to.have.all.keys([
          'action',
          'collectionName',
          'options',
          'query'
        ]);
        expect(request.action).to.equal('beforeSnapshotLookup');
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
      var db = this.db;

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
          db.use('beforeSnapshotLookup', function(request, next) {
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
      var db = this.db;

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
          db.use('beforeSnapshotLookup', function(request, next) {
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

  function expectDocumentToContainFoo(db, valueOfFoo, cb) {
    var query = {_id: 'test1'};

    db.query('testcollection', query, null, null, function(err, results) {
      if (err) return done(err);
      expect(results[0].data).eql({
        foo: valueOfFoo
      });
      cb();
    });
  };
});
