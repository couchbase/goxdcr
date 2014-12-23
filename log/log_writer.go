// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package log

import (
	"fmt"
	"os"
	"sync"
)

// a log writer that performs log file rotation when necessary
type RotatingLogFileWriter struct{
	logFile  *os.File
	maxLogFileSize uint64
    maxNumberOfLogFiles uint64
    mu sync.Mutex
}

func NewRotatingLogFileWriter(fileName string, maxLogFileSize, maxNumberOfLogFiles uint64) (*RotatingLogFileWriter, error) {
	logFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		return nil, err
	}
	return &RotatingLogFileWriter {logFile, maxLogFileSize, maxNumberOfLogFiles, sync.Mutex{}}, nil
}

// implement io.Writer interface
// write data to log. do log file rotation if needed
func (writer *RotatingLogFileWriter) Write(data []byte) (n int, err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()	
	
	fi, err := writer.logFile.Stat()
	if err != nil {
		return
	}
	curSize := fi.Size()
	if curSize + int64(len(data)) < int64(writer.maxLogFileSize) {
		// if not reaching file size limit, simply write to log file
		return writer.logFile.Write(data)
	} else {
		fileName := writer.logFile.Name()
	    // otherwise, perform log file rotation first
		err = writer.rotateLogFiles()
		if err != nil {
			return
		}
		// create a new log file, make it the target file of writer, and write log to it
		writer.logFile, err = os.Create(fileName)
		if err != nil {
			return
		}
		return writer.logFile.Write(data)
	}
}

// get the number of log files by looking for existing log files with the highest postfix  
func (writer *RotatingLogFileWriter) getNumberOfRotatedFiles() (uint64, error){
	for i:= writer.maxNumberOfLogFiles; i >1; i-- {
		rotatedFileName :=  writer.logFile.Name() + "." + fmt.Sprintf("%v", i-1)
		if fileExists(rotatedFileName) {
			return i, nil
		}
	}
	
	// if no log file with postfix is found, there is only one log file
	return 1, nil
}

func (writer *RotatingLogFileWriter) rotateLogFiles() error {
	// close current log file, which will soon get renamed
	fileName := writer.logFile.Name()
	err := writer.logFile.Close()
	if err != nil {
		return err
	}
	
	numOfRotatedFiles, err := writer.getNumberOfRotatedFiles()
	if err != nil {
		return err
	}
	
	numOfRotationsNeeded := numOfRotatedFiles
	if numOfRotationsNeeded == writer.maxNumberOfLogFiles {
		// if number of files have already reached limit, the file with the highest
		// postfix cannot be rotated and will be simply overwritten
		numOfRotationsNeeded --
	}
	// rotate old log files
	for i:= numOfRotationsNeeded; i >0; i-- {
		oldFileName := fileName
		if i > 1 {
			oldFileName = fileName + "." + fmt.Sprintf("%v", i-1)
		}
		newFileName :=  fileName + "." + fmt.Sprintf("%v", i)
		err := os.Rename(oldFileName, newFileName) 
		if err != nil {
			return err
		}
	}
	
	return nil
}

func fileExists(fileName string) bool {
    if _, err := os.Stat(fileName); err != nil {
    	if os.IsNotExist(err) {
        	return false
        }
    }
    return true
}


