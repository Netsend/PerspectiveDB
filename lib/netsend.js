/**
 * Copyright 2014 Netsend.
 *
 * This file is part of Mastersync.
 *
 * Mastersync is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * Mastersync is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with Mastersync. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

// String.repeat polyfill from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/repeat
if (!String.prototype.repeat) {
  /* jshint freeze:false */
  String.prototype.repeat = function (count) {
    if (this === null) { throw new TypeError('can\'t convert ' + this + ' to object'); }
    var str = '' + this;
    count = +count;
    if (count !== count) { count = 0; }
    if (count < 0) { throw new RangeError('repeat count must be non-negative'); }
    if (count === Infinity) { throw new RangeError('repeat count must be less than infinity'); }
    count = Math.floor(count);
    if (str.length === 0 || count === 0) { return ''; }
    // Ensuring count is a 31-bit integer allows us to heavily optimize the
    // main part. But anyway, most current (august 2014) browsers can't handle
    // strings 1 << 28 chars or longer, so :
    if (str.length * count >= 1 << 28) { throw new RangeError('repeat count must not overflow maximum string size'); }
    var rpt = '';
    for (;;) {
      if ((count & 1) === 1) { rpt += str; }
      count >>>= 1;
      if (count === 0) { break; }
      str += str;
    }
    return rpt;
  };
}

function Netsend() {}

module.exports = Netsend;

/**
 * Make sure value has a minimum lenght by prepending characters.
 *
 * @param: {String} val  value to pad
 * @param: {Number, default: 1} [len]  minimum desired length
 * @param: {String, default: ' '} [character]  character to use for padding, spaces
 *                                             by default
 * @param: {Boolean, default: false} [rpad]  whether to pad to the right or pad to
 *                                           the left
 *
 * @return {String} value with minimum length filled up with characters
 */
Netsend.pad = function pad(val, len, character, rpad) {
  if (typeof val !== 'string') { throw new TypeError('val must be a string'); }

  len = len || 1;
  if (typeof len !== 'number') { throw new TypeError('len must be a number'); }

  character = character || ' ';
  if (typeof character !== 'string') { throw new TypeError('character must be a string'); }

  rpad = rpad || false;
  if (typeof rpad !== 'boolean') { throw new TypeError('rpad must be a boolean'); }
  
  len = Math.max(len, val.length);

  if (rpad) {
    return (val + character.repeat(len)).slice(0, len);
  } else {
    return (character.repeat(len) + val).slice(-1 * len);
  }
};
