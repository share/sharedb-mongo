# Mocha test using livedb's snapshot tests
mongoskin = require 'mongoskin'
liveDbMongo = require './mongo'
assert = require 'assert'

# Clear mongo
clear = (callback) ->
  mongo = mongoskin.db 'localhost:27017/test?auto_reconnect', safe:true
  mongo.dropCollection 'testcollection', ->
    mongo.dropCollection 'testcollection_ops', ->
      mongo.close()

      callback()

create = (callback) ->
  clear ->
    callback liveDbMongo 'localhost:27017/test?auto_reconnect', safe: false

describe 'mongo', ->
  afterEach (done) ->
    clear done

  describe 'raw', ->
    beforeEach (done) ->
      @mongo = mongoskin.db 'localhost:27017/test?auto_reconnect', safe:true
      create (@db) => done()

    afterEach ->
      @mongo.close()

    it 'adds an index for ops', (done) -> create (db) =>
      db.writeOp 'testcollection', 'foo', {v:0, create:{type:'json0'}}, (err) =>
        # The problem here is that the index might not have been created yet if
        # the database is busy, which makes this test flakey. I'll put a
        # setTimeout for now, but if there's more problems, it might have to be
        # rewritten.
        setTimeout =>
          @mongo.collection('testcollection_ops').indexInformation (err, indexes) ->
            throw err if err

            # We should find an index with [[ 'name', 1 ], [ 'v', 1 ]]
            for name, idx of indexes
              if JSON.stringify(idx) is '[["name",1],["v",1]]'
                return done()

            throw Error "Could not find index in ops db - #{JSON.stringify(indexes)}"
        , 400

    it 'does not allow editing the system collection', (done) -> create (db) =>
      db.writeSnapshot 'system', 'test', {x:5}, (err) ->
        assert.ok err
        db.getSnapshot 'system', 'test', (err, data) ->
          assert.ok err
          assert.equal data, null
          done()

    describe 'query', ->
      it 'does not allow $where queries in query', (done) -> create (db) =>
        db.query 'unused', 'testcollection', {$where:"true"}, {}, (err, results) ->
          assert.ok err
          assert.equal results, null
          done()

      it 'does not allow $where queries in querydoc', (done) -> create (db) =>
        db.queryDoc 'unused', 'unused', 'testcollection', 'somedoc', {$where:"true"}, (err, results) ->
          assert.ok err
          assert.equal results, null
          done()


  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create

