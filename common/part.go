// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (

)

type Part interface {
	Component
	Connectable
	
	//Start makes goroutine for the part working
	Start (settings map[string]interface{} ) error
	
	//Stop stops the part,
	Stop () error
	
	//Receive accepts data passed down from its upstream
	Receive (data interface {}) error

	//IsStarted returns true if the part is started; otherwise returns false
	IsStarted () bool
	
}