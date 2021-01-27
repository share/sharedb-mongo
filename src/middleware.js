module.exports = extendWithMiddleware;
var MIDDLEWARE_ACTIONS = require('./actions');

function extendWithMiddleware(ShareDbMongo) {
  /**
 * Add middleware to an action or array of actions
 */
  ShareDbMongo.prototype.use = function(action, fn) {
    if (Array.isArray(action)) {
      for (var i = 0; i < action.length; i++) {
        this.use(action[i], fn);
      }
      return this;
    }
    var fns = this.middleware[action] || (this.middleware[action] = []);
    fns.push(fn);
    return this;
  };

  /**
   * Passes request through the middleware stack
   *
   * Middleware may modify the request object. After all middleware have been
   * invoked we call `callback` with `null` and the modified request. If one of
   * the middleware resturns an error the callback is called with that error.
   */
  ShareDbMongo.prototype.trigger = function(action, agent, request, callback) {
    request.action = action;
    if (agent) request.agent = agent;

    var fns = this.middleware[action];
    if (!fns) return callback();

    // Copying the triggers we'll fire so they don't get edited while we iterate.
    fns = fns.slice();
    var next = function(err) {
      if (err) return callback(err);
      var fn = fns.shift();
      if (!fn) return callback();
      fn(request, next);
    };
    next();
  };

  ShareDbMongo.prototype.MIDDLEWARE_ACTIONS = MIDDLEWARE_ACTIONS;
};
