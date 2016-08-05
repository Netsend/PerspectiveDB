'use strict';

module.exports = {
  env: {
    es6: true,
    node: true
  },
  globals: {
    WebSocket: true,
    indexedDB: true
  },
  'extends': 'eslint:recommended',
  rules: {
    'no-console': 'off',
    indent: [ 'error', 2 ],
    'linebreak-style': [ 'error', 'unix' ],
    quotes: [ 'error', 'single' ],
    strict: [ 'error', 'safe' ],
    'comma-dangle': ['error', 'only-multiline']
    //'semi': [ 'error', 'never' ],
    //'semi-spacing': ['error' ]
  }
};
