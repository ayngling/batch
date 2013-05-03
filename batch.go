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
