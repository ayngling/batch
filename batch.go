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

import (
	"appengine"
	"appengine/datastore"
	"errors"
	"reflect"
)

const Size = 500

// batch slice into frames of Size
func DeleteMulti(c appengine.Context, key []*datastore.Key) error {
	// only split into batches if needed
	if len(key) <= Size {
		return datastore.DeleteMulti(c, key)
	}

	var errs []error
	var batch []*datastore.Key
	l := len(key)

	for s, e := 0, 0; s < l; s += Size {
		e = s + Size
		if e >= l {
			e = l
		}

		batch = key[s:e]

		if err := datastore.DeleteMulti(c, batch); err != nil {
			if me, ok := err.(appengine.MultiError); ok {
				if len(errs) == 0 { // lazy init
					errs = make([]error, 0, l)
				}

				for i := range me {
					errs = append(errs, me[i])
				}
			} else {
				return err
			}
		} else if len(errs) > 0 { // no errors, but another batch had errors, so add nils
			for _ = range batch {
				errs = append(errs, nil)
			}
		}
	}

	if len(errs) > 0 {
		return appengine.MultiError(errs) // combined multi-error for the whole set
	}
	return nil
}

func PutMulti(c appengine.Context, key []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	// only split into batches if needed
	if len(key) <= Size {
		return datastore.PutMulti(c, key, src)
	}

	v := reflect.ValueOf(src)
	if v.Kind() != reflect.Slice {
		return nil, errors.New("src is not a slice")
	}

	l := v.Len()

	if len(key) != l {
		return nil, errors.New("length of key and src does not match")
	}

	var batch []*datastore.Key
	var errs []error

	for i, e := 0, 0; i < l; i += Size {
		e = i + Size
		if e >= l {
			e = l
		}

		batch = key[i:e]

		s := reflect.MakeSlice(v.Type(), 0, Size)
		for j := 0; j < Size && i+j < l; j++ {
			s = reflect.Append(s, v.Index(i+j))
		}

		k, err := datastore.PutMulti(c, batch, s.Interface())
		if err != nil {
			if me, ok := err.(appengine.MultiError); ok {
				if len(errs) == 0 { // lazy init
					errs = make([]error, 0, l)
				}

				for i := range me {
					errs = append(errs, me[i])
				}
			} else {
				return nil, err
			}
		} else if len(errs) > 0 { // no errors, but another batch had errors, so add nils
			for _ = range batch {
				errs = append(errs, nil)
			}
		}

		// fill returned keys back in key slice
		for j := i; j < e; j++ {
			key[j] = k[j-i]
		}
	}

	if len(errs) > 0 {
		return key, appengine.MultiError(errs) // combined multi-error for the whole set
	}
	return key, nil
}
