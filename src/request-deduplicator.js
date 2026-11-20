var stringifyPlainJson = require('./stringify-plain-json');

module.exports = RequestDeduplicator;

function RequestDeduplicator() {
  this._callbacksByKey = Object.create(null);
}

RequestDeduplicator.prototype.run = function(keyParts, fetch, callback) {
  var key = stringifyPlainJson(keyParts);
  if (key === undefined) return fetch(callback);

  var callbacks = this._callbacksByKey[key];
  if (callbacks) {
    callbacks.push(callback);
    return;
  }

  callbacks = this._callbacksByKey[key] = [callback];
  var deduplicator = this;
  fetch(function(error, result) {
    delete deduplicator._callbacksByKey[key];
    shareResult(callbacks, fetch, error, result);
  });
};

function shareResult(callbacks, fetch, error, result) {
  scheduleJoiners(callbacks.slice(1), fetch, error, result);
  callbacks[0](error, result);
}

function scheduleJoiners(joiners, fetch, error, result) {
  if (!joiners.length) return;
  var respond = joinerResponse(fetch, error, result);
  joiners.forEach(function(joiner) {
    Promise.resolve().then(function() {
      respond(joiner);
    });
  });
}

function joinerResponse(fetch, error, result) {
  if (error) {
    return function(joiner) {
      joiner(error);
    };
  }
  var resultJson = stringifyPlainJson(result);
  if (resultJson === undefined) return fetch;
  return function(joiner) {
    joiner(null, JSON.parse(resultJson));
  };
}
