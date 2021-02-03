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

  describe('beforeWrite', function() {
    it('should augment query filter and write to the document when commit is called', function(done) {
      var db = this.db;
      // Augment the queryFilter. The original query looks up the document by id, wheras this middleware
      // changes it to use the `foo` property. The end result still returns the same document. The next
      // middleware ensures we attached it to the request.
      // We can't truly change which document is returned from the query because MongoDB will not allow
      // the immutable fields such as `_id` to be changed.
      db.use('beforeWrite', function(request, next) {
        request.queryFilter.foo = 'bar';
        next();
      });
      // Attach this middleware to check that the original one is passing the context
      // correctly. Commit will be called after this.
      db.use('beforeWrite', function(request, next) {
        expect(request.queryFilter).to.deep.equal({
          _id: 'test1',
          _v: 1,
          foo: 'bar'
        });
        next();
      });

      var snapshot = {type: 'json0', id: 'test1', v: 1, data: {foo: 'bar'}};
      var query = {_id: 'test1'};

      function findsTest1(valueOfFoo, cb) {
        db.query('testcollection', query, null, null, function(err, results) {
          if (err) return done(err);

          expect(results[0].data).eql({
            foo: valueOfFoo
          });

          cb();
        });
      };

      var editOp = {v: 2, op: [{p: ['foo'], oi: 'bar', oi: 'baz'}], m: {ts: Date.now()}};
      var newSnapshot = {type: 'json0', id: 'test1', v: 2, data: {foo: 'fuzz'}};

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, function(err) {
        if (err) return done(err);
        findsTest1('bar', function() {
          db.commit('testcollection', snapshot.id, editOp, newSnapshot, null, function(err) {
            if (err) return done(err);
            // Ensure the value is updated as expected
            findsTest1('fuzz', done);
          });
        });
      });
    });
  });
});
