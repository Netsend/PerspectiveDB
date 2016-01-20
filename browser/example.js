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

// connection view/handler
function connectionManager(pdb) {
  var ul = document.querySelector('ul#connections');
  var conns = pdb.connections();

  Object.keys(conns).forEach(function(name) {
    var stat = conns[name];

    var li = document.createElement('li');
    var button = document.createElement('button');
    li.className = 'disconnected';
    li.textContent = stat.name;
    li.title = stat.uri;
    button.textContent = (stat.status === 'connected') ? 'disconnect' : 'connect';

    li.appendChild(button);
    ul.appendChild(li);

    button.onclick = function(ev) {
      if (li.classList.contains('connected')) {
        pdb.disconnect(function(err) {
          if (err) {
            console.error('error disconnecting', err);
            msg.textContent = err.message;
            return;
          }
          li.className = 'disconnected';
          button.textContent = 'connect';
        });
      } else {
        pdb.connect(function(err) {
          if (err) {
            console.error('error connecting', err);
            msg.textContent = err.message;
            return;
          }
          li.className = 'connected';
          button.textContent = 'disconnect';
        });
      }
    };
  });
}

// persdb view
function pdbView(reader) {
  var table  = document.querySelector('table#persdb');

  reader.on('readable', function() {
    var item = reader.read();
    if (item == null) { return; }

    var tr = document.createElement('tr');
    var td;
    td = document.createElement('td');
    td.textContent = JSON.stringify(item.h);
    tr.appendChild(td);

    table.appendChild(tr);
  });
}

// main program
function main(db, pdb) {
  var msg     = document.querySelector('#msg');

  var table   = document.querySelector('table#idb tbody');

  var form    = document.querySelector('form');

  var ssn     = document.querySelector('#addCustomerSsn');
  var name    = document.querySelector('#addCustomerName');
  var email   = document.querySelector('#addCustomerEmail');
  var age     = document.querySelector('#addCustomerAge');

  pdb.on('error', function(err) {
    console.error('error disconnecting', err);
    msg.textContent = err.message;
  });

  connectionManager(pdb);

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

  pdb.on('merge', function(item) {
    console.log('new version merged', item);
  });
}

if (typeof config !== 'object') { throw new Error('make sure config is set'); }
if (typeof PersDB !== 'function') { throw new Error('make sure PersDB is loaded'); }

// open db and write an object
var req = indexedDB.open('MyTestDatabase');

req.onsuccess = function(ev) {
  var db = ev.target.result;

  // start PersDB
  var opts = {
    perspectives: [config],
    mergeInterval: 0
  };
  var pdb = new PersDB(db, opts);
  main(db, pdb);

  var reader = pdb.createReadStream({ tail: false });
  pdbView(reader);
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
