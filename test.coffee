# Mocha test using livedb's snapshot tests
mongoskin = require 'mongoskin'
liveDbMongo = require './mongo'

create = (callback) ->

  # Clear mongo
  mongo = mongoskin.db 'localhost:27017/test?auto_reconnect', safe:true
  mongo.dropCollection 'users', ->
    mongo.dropCollection 'users ops', ->
      mongo.close()

      callback liveDbMongo 'localhost:27017/test?auto_reconnect', safe: false

describe 'mongo', ->
  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create

