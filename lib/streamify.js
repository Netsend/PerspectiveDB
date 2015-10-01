/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Tim Kuijsten
 * Copyright (c) 2013 Matthew I. Metnetsky
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

'use strict';

var Readable = require('readable-stream').Readable
;

function StreamArray(list) {
    if (!Array.isArray(list)) {
        throw new TypeError('First argument must be an Array');
    }

    Readable.call(this, {objectMode:true});

    this._i = 0;
    this._l = list.length;
    this._list = list;
}

StreamArray.prototype = Object.create(Readable.prototype, {constructor: {value: StreamArray}});

StreamArray.prototype._read = function() {
  var that = this;
  process.nextTick(function() {
    that.push(that._i < that._l ? that._list[that._i++] : null);
  });
};

module.exports = function(list) {
    return new StreamArray(list);
};
