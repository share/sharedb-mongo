var OpLinkValidator = require('../op-link-validator');
var expect = require('chai').expect;

describe('OpLinkValidator', function() {
  it('starts with no unique op', function() {
    var validator = new OpLinkValidator();
    var opWithUniqueVersion = validator.opWithUniqueVersion();
    expect(opWithUniqueVersion).to.equal(null);
  });

  it('starts not at the end of the list', function() {
    var validator = new OpLinkValidator();
    expect(validator.isAtEndOfList()).to.equal(false);
  });

  it('has no unique op with just one op', function() {
    var op = {v: 1};
    var validator = new OpLinkValidator();

    validator.push(op);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(null);
  });

  it('has a unique op with just two different ops', function() {
    var op1 = {v: 1};
    var op2 = {v: 2};
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(op1);
  });

  it('does not have a uniquye op with just two identical ops', function() {
    var op1 = {v: 1};
    var op2 = {v: 1};
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(null);
  });

  it('has a unique op with three ops with different versions', function() {
    var op1 = {v: 1};
    var op2 = {v: 2};
    var op3 = {v: 3};
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    validator.push(op3);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(op2);
  });

  it('is not at the end of the list with three ops', function() {
    var op1 = {v: 1};
    var op2 = {v: 2};
    var op3 = {v: 3};
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    validator.push(op3);

    expect(validator.isAtEndOfList()).to.equal(false);
  });

  it('does not have a unique op with three ops with the same version', function() {
    var op = {v: 1};
    var validator = new OpLinkValidator();

    validator.push(op);
    validator.push(op);
    validator.push(op);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(null);
  });

  it('does not have a unique op if the first two ops are the same', function() {
    var op1 = {v: 1};
    var op2 = {v: 1};
    var op3 = {v: 2};
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    validator.push(op3);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(null);
  });

  it('does not have a unique op if the last two ops are the same', function() {
    var op1 = {v: 1};
    var op2 = {v: 2};
    var op3 = {v: 2};
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    validator.push(op3);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(null);
  });

  it('has a unique op in a long chain', function() {
    var op1 = {v: 1};
    var op2 = {v: 1};
    var op3 = {v: 1};
    var op4 = {v: 2};
    var op5 = {v: 2};
    var op6 = {v: 3};
    var op7 = {v: 4};
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    validator.push(op3);
    validator.push(op4);
    validator.push(op5);
    validator.push(op6);
    validator.push(op7);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(op6);
  });

  it('has a unique op with two ops and a current op of null', function() {
    var op1 = {v: 1};
    var op2 = {v: 2};
    var op3 = null;
    var validator = new OpLinkValidator();

    validator.push(op1);
    validator.push(op2);
    validator.push(op3);
    var opWithUniqueVersion = validator.opWithUniqueVersion();

    expect(opWithUniqueVersion).to.equal(op2);
  });

  it('is at the end of the list with a current op of null', function() {
    var op = null;
    var validator = new OpLinkValidator();
    validator.push(op);
    expect(validator.isAtEndOfList()).to.equal(true);
  });
});
