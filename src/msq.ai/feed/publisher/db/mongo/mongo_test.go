package mongo

import (
	"gotest.tools/assert"
	"msq.ai/data"
	"testing"
)

func TestSplitByBatch1(t *testing.T) {

	var buf [bufferSize]*data.Quote

	ret := splitByBatch(buf[0:0], 0, 10)

	assert.Equal(t, len(ret), 0)
}

func TestSplitByBatch2(t *testing.T) {

	var buf [bufferSize]*data.Quote

	buf[0] = &data.Quote{}

	ret := splitByBatch(buf[0:1], 1, 10)

	assert.Equal(t, len(ret), 1)
	assert.Equal(t, len(ret[0]), 1)

}
