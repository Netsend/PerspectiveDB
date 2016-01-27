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
  var ul  = document.querySelector('ul#connections');

  var conns = pdb.connections();

  pdb.on('connection:connect', function(pe) {
    var li = ul.querySelector('li.' + pe);
    if (!li) { return; }

    li.classList.remove('disconnecting');
    li.classList.remove('disconnected');
    li.classList.add('connected');
    var button = li.querySelector('button');
    button.textContent = 'disconnect';
  });

  pdb.on('connection:disconnect', function(pe) {
    var li = ul.querySelector('li.' + pe);
    if (!li) { return; }

    li.classList.remove('connecting');
    li.classList.remove('connected');
    li.classList.add('disconnected');
    var button = li.querySelector('button');
    button.textContent = 'connect';
  });

  Object.keys(conns).forEach(function(name) {
    var stat = conns[name];

    var li     = document.createElement('li');
    var button = document.createElement('button');
    var span   = document.createElement('span');

    li.classList.add(name);
    li.classList.add('disconnected');

    li.textContent = stat.name;
    li.title = stat.uri;

    button.textContent = (stat.status === 'connected') ? 'disconnect' : 'connect';


    li.appendChild(button);
    li.appendChild(span);
    ul.appendChild(li);

    button.onclick = function(ev) {
      span.textContent = '';

      if (li.classList.contains('connected')) {
        li.classList.remove('connected');
        li.classList.add('disconnecting');
        button.disabled = true;
        button.textContent = 'disconnecting';

        pdb.disconnect(function(err) {
          li.classList.remove('disconnecting');
          button.disabled = false;

          if (err) {
            li.classList.add('connected');
            button.textContent = 'disconnect';

            span.textContent = 'error disconnecting';
            return;
          }
          li.classList.remove('connected');
          li.classList.add('disconnected');
          button.textContent = 'connect';
        });
      } else {
        li.classList.remove('disconnected');
        li.classList.add('connecting');
        button.disabled = true;
        button.textContent = 'connecting';

        pdb.connect(stat.name, function(err) {
          li.classList.remove('connecting');
          button.disabled = false;

          if (err) {
            li.classList.add('disconnected');
            button.textContent = 'connect';

            span.textContent = 'error connecting';
            return;
          }
          li.classList.remove('disconnected');
          li.classList.add('connected');
          button.textContent = 'disconnect';
        });
      }
    };
  });
}

// create a new pdb tree view
function createPdbTable(name) {
  var table = document.createElement('table');
  table.id = name;

  var caption = document.createElement('caption');
  caption.textContent = name;

  var thead = document.createElement('thead');
  thead.innerHTML = '<tr> <th>id</th> <th>parents</th> <th>version</th> <th>flags</th> <th>perspective</th> <th>doc</th> <th>meta</th> </tr>';

  var tbody = document.createElement('tbody');

  table.appendChild(caption);
  table.appendChild(thead);
  table.appendChild(tbody);

  return table;
}

// add item to persdb view
function createPdbTableRow(item) {
  var tr = document.createElement('tr');
  var td;

  td = document.createElement('td');
  td.textContent = JSON.stringify(item.h.id);
  tr.appendChild(td);

  td = document.createElement('td');
  td.textContent = JSON.stringify(item.h.pa);
  tr.appendChild(td);

  td = document.createElement('td');
  td.textContent = item.h.v;
  tr.appendChild(td);

  td = document.createElement('td');
  td.classList.add('flags');
  var attrs = [];
  if (item.h.c) { attrs.push('c'); }
  if (item.h.d) { attrs.push('d'); }
  td.textContent = attrs.join(' ');
  tr.appendChild(td);

  td = document.createElement('td');
  td.textContent = JSON.stringify(item.m);
  tr.appendChild(td);

  td = document.createElement('td');
  td.textContent = JSON.stringify(item.b);
  tr.appendChild(td);

  return tr;
}

// persdb view
function appendToTable(table, reader) {
  var tbody = table.querySelector('tbody');
  reader.on('readable', function() {
    var item = reader.read();
    if (item == null) { return; }

    table.appendChild(createPdbTableRow(item));
  });
}

// main program
function main(db, pdb) {
  var msg      = document.querySelector('#msg');

  var idbTable = document.querySelector('table#idb tbody');
  var pdbDiv   = document.querySelector('div#persdb');

  var form     = document.querySelector('form');

  var ssn      = document.querySelector('#addCustomerSsn');
  var name     = document.querySelector('#addCustomerName');
  var email    = document.querySelector('#addCustomerEmail');
  var age      = document.querySelector('#addCustomerAge');

  // create a table for the local tree
  var localTable = createPdbTable('local');
  appendToTable(localTable, pdb.createReadStream({ tail: false }));

  // create view of internal stage tree using the private api
  var stageTable = createPdbTable('stage');
  appendToTable(stageTable, pdb._mt.getStageTree().createReadStream({ tail: false }));

  // append tables of different trees
  pdbDiv.appendChild(localTable);
  pdbDiv.appendChild(stageTable);

  // do the same for every perspective
  var perspectives = pdb.getPerspectives();
  perspectives.forEach(function(pe) {
    // create view of pe tree using the private api
    var table = createPdbTable(pe);
    appendToTable(table, pdb._mt._pe[pe].createReadStream({ tail: false }));
    pdbDiv.appendChild(table);
  });

  pdb.on('error', function(err) {
    console.error('error disconnecting', err);
    msg.textContent = err.message;
  });

  pdb.on('data', function(item) {
    localTable.appendChild(createPdbTableRow(item));
    reloadCustomersList();
  });

  // setup connection manager
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
  idbTable.onclick = function(ev) {
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
    reloadList(db, 'customers', idbTable, function(err) {
      if (err) {
        console.error(err);
        msg.textContent = err.message;
        return;
      }
    });
  }

  reloadCustomersList();
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
    conflictHandler: function(attrs, newHead, lhead, cb) {
      console.error('merge conflict', attrs, newHead, lhead);
      var msg = document.querySelector('#msg');
      msg.textContent = 'Error merging ' +  JSON.stringify(newHead) + ' with ' + JSON.stringify(lhead) + ': ' + attrs.join(' ');
    }
  };
  var pdb = new PersDB(db, opts);
  main(db, pdb);
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
