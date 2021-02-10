var MIDDLEWARE_ACTIONS = require('./actions');

function MiddlewareHandler() {
  this._middleware = {};
}

/**
* Add middleware to an action or array of actions
*
* @param action The action to use from MIDDLEWARE_ACTIONS (e.g. 'beforeOverwrite')
* @param fn The function to call when this middleware is triggered
* The fn receives a request object with information on the triggered action (e.g. the snapshot to write)
* and a next function to call once the middleware is complete
*
* NOTE: It is recommended not to add async or long running tasks to the sharedb-mongo middleware as it will
* be called very frequently during sensitive operations. It may have a significant performance impact.
*/
MiddlewareHandler.prototype.use = function(action, fn) {
  if (Array.isArray(action)) {
    for (var i = 0; i < action.length; i++) {
      this.use(action[i], fn);
    }
    return this;
  }
  if (!action) throw new Error('Expected action to be defined');
  if (!fn) throw new Error('Expected fn to be defined');
  if (!Object.values(MIDDLEWARE_ACTIONS).includes(action)) {
    throw new Error('Unrecognized action name ' + action);
  }

  var fns = this._middleware[action] || (this._middleware[action] = []);
  fns.push(fn);
  return this;
};

/**
 * Passes request through the middleware stack
 *
 * Middleware may modify the request object. After all middleware have been
 * invoked we call `callback` with `null` and the modified request. If one of
 * the middleware resturns an error the callback is called with that error.
 *
 * @param action The action to trigger from MIDDLEWARE_ACTIONS (e.g. 'beforeOverwrite')
 * @param request Request details such as the snapshot to write, depends on the triggered action
 * @param callback Function to call once the middleware has been processed.
 */
MiddlewareHandler.prototype.trigger = function(action, request, callback) {
  request.action = action;

  var fns = this._middleware[action];
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

MiddlewareHandler.Actions = MIDDLEWARE_ACTIONS;

module.exports = MiddlewareHandler;
