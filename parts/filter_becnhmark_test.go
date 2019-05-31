package parts

import (
	"fmt"
	"testing"
)

func BenchmarkFilterUprEvent(b *testing.B) {
	fmt.Println("============== Test case start: BenchmarkFilterUprEvent =================")
	perfDataUprEvent, _ := RetrieveUprFile("../utils/testFilteringData/xattrSlice.bin")
	benchFilter, _ := NewFilter(filterId, "META().xattrs.stringType EXISTS", realUtil)

	for n := 0; n < b.N; n++ {
		benchFilter.FilterUprEvent(perfDataUprEvent)
	}

	fmt.Println("============== Test case end: BenchmarkFilterUprEvent =================")
}
