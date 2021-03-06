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
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"reflect"
)

var (
	SizeGet    int = 1000
	SizePut    int = 500
	SizeDelete int = 500
)

// batch slice into frames of Size
func DeleteMulti(c context.Context, key []*datastore.Key) error {
	l := len(key)

	// only split into batches if needed
	if l <= SizeDelete {
		return datastore.DeleteMulti(c, key)
	}

	var errs []error
	var batch []*datastore.Key

	for s, e := 0, 0; s < l; s += SizeDelete {
		e = s + SizeDelete
		if e >= l {
			e = l
		}

		batch = key[s:e]

		if err := datastore.DeleteMulti(c, batch); err != nil {
			if me, ok := err.(appengine.MultiError); ok {
				if len(errs) == 0 { // lazy init
					errs = make([]error, s, l) // add nils for previous batches
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

func PutMulti(c context.Context, key []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	// only split into batches if needed
	if len(key) <= SizePut {
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

	var errs []error

	for i, e := 0, 0; i < l; i += SizePut {
		e = i + SizePut
		if e >= l {
			e = l
		}

		k, err := datastore.PutMulti(c, key[i:e], v.Slice(i, e).Interface())
		if err != nil {
			if me, ok := err.(appengine.MultiError); ok {
				if len(errs) == 0 { // lazy init
					errs = make([]error, i, l) // add nils for previous batches
				}
				for i := range me {
					errs = append(errs, me[i])
				}
			} else {
				return nil, err
			}
		} else if len(errs) > 0 { // no errors, but another batch had errors, so add nils
			for _ = range key[i:e] {
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

func GetMulti(c context.Context, key []*datastore.Key, src interface{}) error {
	if len(key) <= SizeGet {
		return datastore.GetMulti(c, key, src)
	}

	// Validate the input interface
	v := reflect.ValueOf(src)
	if v.Kind() != reflect.Slice {
		return errors.New("src is not a slice")
	} else if len(key) != v.Len() {
		return errors.New("length of key and src does not match")
	}

	var errs []error
	l := len(key)

	for i, e := 0, 0; i < l; i += SizeGet {
		e = i + SizeGet
		if e > l {
			e = l
		}

		err := datastore.GetMulti(c, key[i:e], v.Slice(i, e).Interface())
		if err != nil {
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
			for _ = range key[i:e] {
				errs = append(errs, nil)
			}
		}
	}

	if len(errs) > 0 {
		return appengine.MultiError(errs) // combined multi-error for the whole set
	}

	return nil
}
