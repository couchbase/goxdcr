package mcdebug

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMcopsDataRace(t *testing.T) {
	mcSent := &mcops{}
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		mcSent.count(1, 2, fmt.Errorf("mcdebug: throw some error")) // write
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println(mcSent.String()) // concurrent read
	}()

	wg.Wait()
	require.NotEqual(t, `{"bytes":{},"errs":{},"ops":{}}`, mcSent.String())
	fmt.Println(mcSent.String())
}
