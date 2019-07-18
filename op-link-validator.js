/**
 * This is a class for determining an op with a unique version number
 * when presented with an **ordered** series of ops.
 *
 * For example, consider the following chain of op versions:
 * 1 -> 1 -> 2 -> 2 -> 3 -> 4
 * If we want to find the first unique version, we must consider a
 * window of three versions. For example, if we consider the first
 * three versions:
 * 1 -> 1 -> 2
 * Then we know that 1 is not unique. We don't know if 2 is unique
 * yet, because we don't know what comes next. Therefore we push
 * one more version and check again:
 * 1 -> 2 -> 2
 * Again we now see that 2 is not unique, so we keep pushing ops
 * until we reach the final window:
 * 2 -> 3 -> 4
 * From here, **assuming the ops are well ordered** we can safely
 * see that v3 is unique. We cannot make the same assumption of
 * v4, because we don't know what comes next.
 *
 * Note that we also assume that the chain starts with **all**
 * of the copies of an op version. That is that if we are provided
 * 1 -> 2
 * Then v1 is unique (because there are no other v1s).
 *
 * Similarly, if a null op is pushed into the class, it is assumed
 * to be the end of the chain, and hence a unique version can be
 * inferred, eg with this chain:
 * 5 -> 6 -> null
 * We say that 6 is unique, because we've reached the end of the
 * list
 */
function OpLinkValidator() {
  this.currentOp = undefined;
  this.previousOp = undefined;
  this.oneBeforePreviousOp = undefined;
}

OpLinkValidator.prototype.push = function(op) {
  this.oneBeforePreviousOp = this.previousOp;
  this.previousOp = this.currentOp;
  this.currentOp = op;
};

OpLinkValidator.prototype.opWithUniqueVersion = function() {
  return this._previousVersionWasUnique() ? this.previousOp : null;
};

OpLinkValidator.prototype.isAtEndOfList = function() {
  // We ascribe a special meaning to a current op of null
  // being that we're at the end of the list, because this
  // is the value that the Mongo cursor will return when
  // the cursor is exhausted
  return this.currentOp === null;
};

OpLinkValidator.prototype._previousVersionWasUnique = function() {
  var previousVersion = this._previousVersion();

  return typeof previousVersion === 'number'
    && previousVersion !== this._currentVersion()
    && previousVersion !== this._oneBeforePreviousVersion();
};

OpLinkValidator.prototype._currentVersion = function() {
  return this.currentOp && this.currentOp.v;
};

OpLinkValidator.prototype._previousVersion = function() {
  return this.previousOp && this.previousOp.v;
};

OpLinkValidator.prototype._oneBeforePreviousVersion = function() {
  return this.oneBeforePreviousOp && this.oneBeforePreviousOp.v;
};

module.exports = OpLinkValidator;
