module.exports = stringifyPlainJson;

function stringifyPlainJson(value) {
  try {
    return JSON.stringify(value, rejectNonPlainJson);
  } catch (error) {
    if (error instanceof TypeError) return undefined;
    throw error;
  }
}

function rejectNonPlainJson(key, valueAfterToJson) {
  var originalValue = this[key];
  if (!isPlainJsonValue(originalValue)) throw new TypeError('Not plain JSON');
  return valueAfterToJson;
}

function isPlainJsonValue(value) {
  if (value === null) return true;
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return true;
    case 'number':
      return isFinite(value);
    case 'object':
      return isPlainContainer(value);
    default:
      return false;
  }
}

function isPlainContainer(value) {
  if ('toJSON' in value) return false;
  var prototype = Object.getPrototypeOf(value);
  return prototype === Array.prototype ||
    prototype === Object.prototype ||
    prototype === null;
}
