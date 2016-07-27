/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

// create an id
function generateId(objectStore, key) {
  return objectStore + '\x01' + key;
}

// extract an id
function idFromId(id) {
  // expect only one 0x01
  return id.split('\x01', 2)[1];
}

// extract an object store from an id
function objectStoreFromId(id) {
  // expect only one 0x01
  return id.split('\x01', 1)[0];
}

module.exports = { generateId, idFromId, objectStoreFromId };
