package common

import (
	"net/http"
	"fmt"
	"errors"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/utils"
)

func GetAdminportUrlPrefix(hostName string) string {
	return "http://" + utils.GetHostAddr(hostName, base.AdminportNumber) + base.AdminportUrlPrefix
}

func ValidateResponse(testName string, response *http.Response, err error) error {
	if err != nil || response.StatusCode != 200 {
		errMsg := fmt.Sprintf("Test %v failed. err=%v", testName, err)
		if response != nil {
			errMsg += fmt.Sprintf("; response status=%v", response.Status)
		}
		errMsg += "\n"
		return errors.New(errMsg)
	}
	return nil
}

func ValidateFieldValue(fieldName string, expectedValue, actualValue interface{}) error {
	if expectedValue != actualValue {
		return errors.New(fmt.Sprintf("Incorrect value in field %v. Expected value=%v, actual value=%v\n", fieldName, expectedValue, actualValue))
	}
	return nil
}

