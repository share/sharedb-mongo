var expect = require('chai').expect;

describe('mocha config', function() {
  it('applies our configured timeout, and not the 2s default', function() {
    expect(this.timeout()).to.be.at.least(18000);
  });
});
