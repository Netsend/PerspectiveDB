// config.js (c) 2010-2014 Loren West and other contributors
// May be freely distributed under the MIT license.
// For further details and documentation:
// http://lorenwest.github.com/node-config

'use strict';

/**
* Underlying get mechanism
*
* @private
* @method getImpl
* @param object {object} - Object to get the property for
* @param property {string | array[string]} - The property name to get (as an array or '.' delimited string)
* @return value {mixed} - Property value, including undefined if not defined.
*/
function get(object, property) {
  var elems = Array.isArray(property) ? property : property.split('.'),
  name = elems[0],
  value = object[name];
  if (elems.length <= 1) {
    return value;
  }
  if (typeof value !== 'object') {
    return undefined;
  }
  return get(value, elems.slice(1));
}

module.exports = get;
