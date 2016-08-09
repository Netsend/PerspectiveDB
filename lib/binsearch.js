'use strict';

// from http://stackoverflow.com/questions/22697936/binary-search-in-javascript/29018745#29018745
function binarySearch(ar, el, compare_fn) {
  var m = 0;
  var n = ar.length - 1;
  while (m <= n) {
    var k = (n + m) >> 1;
    var cmp = compare_fn(el, ar[k]);
    if (cmp > 0) {
      m = k + 1;
    } else if(cmp < 0) {
      n = k - 1;
    } else {
      return k;
    }
  }
  return -m - 1;
}

function comparator(a, b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

// make sure ar is sorted
function inArray(ar, el) {
  return binarySearch(ar, el, comparator) >= 0;
}

module.exports = { inArray };
