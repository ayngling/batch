/*
   Copyright 2015 Alexander Yngling

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
	"math/rand"
	"time"
)

type RetryError struct {
	err error
}

func (this *RetryError) Error() string {
	return this.err.Error()
}

func (this *RetryError) Actual() error {
	return this.err
}

func NewRetryError(err error) *RetryError {
	return &RetryError{err}
}

func IsRetryError(err error) bool {
	_, ok := err.(*RetryError)
	return ok
}

type RetryOptions struct {
	Retries      int
	InitialDelay time.Duration
	Backoff      float64
	Rand         *rand.Rand // in case caller wants to reuse a rand generator
}

var defaultRetryOptions = &RetryOptions{Retries: 5, InitialDelay: 100 * time.Millisecond, Backoff: 2.0, Rand: nil}

func Retry(c appengine.Context, fn func(c appengine.Context) error, o *RetryOptions) error {
	if o == nil { // use defaults
		o = defaultRetryOptions
	} else { // fill in defaults
		if o.Retries == 0 {
			o.Retries = defaultRetryOptions.Retries
		}
		if o.InitialDelay == 0 {
			o.InitialDelay = defaultRetryOptions.InitialDelay
		}
		if o.Backoff == 0 {
			o.Backoff = defaultRetryOptions.Backoff
		}
	}

	var awhile time.Duration = 0 // lazy init
	var fuzz *rand.Rand
	retries := o.Retries
	for {
		if err := fn(c); err == nil {
			return nil
		} else if appengine.IsTimeoutError(err) || IsRetryError(err) {

			// randomized exponential backoff policy (cf. https://cloud.google.com/appengine/articles/scalability#backoff )
			if retries == 0 { // give up after retries
				return err
			} else if awhile == 0 { // lazy init
				awhile = o.InitialDelay
				if o.Rand == nil {
					fuzz = rand.New(rand.NewSource(time.Now().Unix())) // default behaviour is to create new rand if needed
				} else {
					fuzz = o.Rand // use caller-provided rand
				}
			} else {
				awhile = time.Duration(float64(awhile) * o.Backoff)
			}
			time.Sleep(time.Duration((fuzz.Float64() / 2.0 + 0.75) * float64(awhile))) // random component to avoid thundering herd problem (values taken from https://github.com/GoogleCloudPlatform/appengine-gcs-client/blob/master/java/src/main/java/com/google/appengine/tools/cloudstorage/RetryHelper.java )
			retries--
			continue
		} else {
			return err
		}
	}
}
