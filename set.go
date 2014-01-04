/*
   Copyright 2012 Alexander Yngling

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package batch

import "appengine/datastore"

const DefaultSize = 5

func New(size int) ([]*datastore.Key, []interface{}) {
	return make([]*datastore.Key, 0, size), make([]interface{}, 0, size)
}

// If keys or vals are nil, they will be created with deafult size
func Add(keys []*datastore.Key, vals []interface{}, key *datastore.Key, val interface{}) ([]*datastore.Key, []interface{}) {
	if keys == nil {
		keys = make([]*datastore.Key, 0, DefaultSize)
	}
	if vals == nil {
		vals = make([]interface{}, 0, DefaultSize)
	}

	keys = append(keys, key)
	vals = append(vals, val)
	return keys, vals
}
