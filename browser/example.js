/* jshint -W097 */

'use strict';

// iterate over all object store entries
function list(db, osName, iterator, cb) {
  var tr = db.transaction(osName, 'readonly');
  tr.oncomplete = function(ev) {
    cb(null);
  };
  tr.onerror = function(ev) {
    cb(ev.target.error);
  };
  tr.objectStore(osName).openCursor().onsuccess = function(ev) {
    var cursor = ev.target.result;
    if (!cursor) { return; }

    iterator(cursor.value, function(cont) {
      if (cont != null && cont === false) {
        tr.abort();
      } else {
        cursor.continue();
      }
    });
  };
}

function createCustomerTableRow(item) {
  var tr = document.createElement('tr');
  var td;
  td = document.createElement('td');
  td.textContent = item.ssn;
  tr.appendChild(td);

  td = document.createElement('td');
  td.textContent = item.name;
  tr.appendChild(td);

  td = document.createElement('td');
  td.textContent = item.email;
  tr.appendChild(td);

  td = document.createElement('td');
  td.textContent = item.age;
  tr.appendChild(td);

  td = document.createElement('td');
  td.className = 'remove';
  td.textContent = '\u{1f5d1}'; // Unicode WASTEBASKET
  tr.appendChild(td);

  return tr;
}

function reloadList(db, osName, el, cb) {
  el.innerHTML = '';
  list(db, osName, function(item, next) {
    el.appendChild(createCustomerTableRow(item));
    next();
  }, cb);
}

// main program
function main(db) {
  var msg    = document.querySelector('#msg');

  var table  = document.querySelector('tbody');

  var form   = document.querySelector('form');

  var ssn    = document.querySelector('#addCustomerSsn');
  var name   = document.querySelector('#addCustomerName');
  var email  = document.querySelector('#addCustomerEmail');
  var age    = document.querySelector('#addCustomerAge');

  // handle form
  form.onsubmit = function(ev) {
    // clear messages
    msg.textContent = '';

    var obj = {
      ssn:   ssn.value.trim(),
      name:  name.value.trim(),
      age:   age.value.trim(),
      email: email.value.trim()
    };

    var tr = db.transaction(['customers'], 'readwrite');
    tr.oncomplete = function(ev) {
      msg.textContent = 'customer ' + obj.ssn + ' saved';
    };
    tr.onerror = function(ev) {
      console.error('error setting customer', ev.target.error);
      msg.textContent = ev.target.error.message;
    };
    var os = tr.objectStore('customers');

    os.put(obj);

    // refresh list
    reloadCustomersList();

    ev.stopPropagation();
    return false;
  };

  // handle list deletion clicks
  table.onclick = function(ev) {
    // clear messages
    msg.textContent = '';

    var el = ev.target;
    if (el.classList.contains('remove')) {
      // remove is clicked
      // expect primary key in first column of row
      var id = el.parentNode.querySelector('td').textContent;

      var tr = db.transaction(['customers'], 'readwrite');
      tr.oncomplete = function(ev) {
        msg.textContent = 'customer ' + id + ' removed';
      };
      tr.onerror = function(ev) {
        console.error('error setting customer', ev.target.error);
        msg.textContent = ev.target.error.message;
      };
      var os = tr.objectStore('customers');

      os.delete(id);

      // refresh list
      reloadCustomersList();

      ev.stopPropagation();
      return false;
    }
  };

  // show list entries
  function reloadCustomersList() {
    // refill
    reloadList(db, 'customers', table, function(err) {
      if (err) {
        console.error(err);
        msg.textContent = err.message;
        return;
      }
    });
  }

  reloadCustomersList();
}

var config = require('./config.json');
var PersDB = require('./db');

// open db and write an object
var req = indexedDB.open('MyTestDatabase');

req.onsuccess = function(ev) {
  var db = ev.target.result;

  var opts = { perspectives: config };

  // start PersDB
  PersDB(db, opts, function(err) {
    if (err) { throw err; }
    main(db);
  });
};

req.onerror = function(ev) {
  console.error('err', ev.data);
};

// from https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API/Using_IndexedDB
req.onupgradeneeded = function(ev) {
  console.log('upgradeneeded');

  var db = ev.target.result;

  // Create an objectStore to hold information about our customers. We're
  // going to use "ssn" as our key path because it's guaranteed to be
  // unique - or at least that's what I was told during the kickoff meeting.
  var objectStore = db.createObjectStore('customers', { keyPath: 'ssn' });

  // Create an index to search customers by name. We may have duplicates
  // so we can't use a unique index.
  objectStore.createIndex('name', 'name', { unique: false });

  // Create an index to search customers by email. We want to ensure that
  // no two customers have the same email, so use a unique index.
  objectStore.createIndex('email', 'email', { unique: true });
};
