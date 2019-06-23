package mongo

import (
	"gotest.tools/assert"
	"msq.ai/data"
	"testing"
)

func TestSplitByBatch1(t *testing.T) {

	var buf [bufferSize]*data.Quote

	ret := splitByBatch(buf[0:0], 10)

	assert.Equal(t, len(ret), 0)
}

func TestSplitByBatch2(t *testing.T) {

	var buf [bufferSize]*data.Quote

	buf[0] = &data.Quote{}

	ret := splitByBatch(buf[0:1], 10)

	assert.Equal(t, len(ret), 1)
	assert.Equal(t, len(ret[0]), 1)
}

func TestSplitByBatch3(t *testing.T) {

	var buf [bufferSize]*data.Quote

	buf[0] = &data.Quote{}
	buf[1] = &data.Quote{}
	buf[2] = &data.Quote{}
	buf[3] = &data.Quote{}

	ret := splitByBatch(buf[0:4], 4)

	assert.Equal(t, len(ret), 1)
	assert.Equal(t, len(ret[0]), 4)
}

func TestSplitByBatch4(t *testing.T) {

	var buf [bufferSize]*data.Quote

	buf[0] = &data.Quote{}
	buf[1] = &data.Quote{}
	buf[2] = &data.Quote{}
	buf[3] = &data.Quote{}

	ret := splitByBatch(buf[0:4], 2)

	assert.Equal(t, len(ret), 2)
	assert.Equal(t, len(ret[0]), 2)
	assert.Equal(t, len(ret[1]), 2)
}

func TestSplitByBatch5(t *testing.T) {

	var buf [bufferSize]*data.Quote

	buf[0] = &data.Quote{}
	buf[1] = &data.Quote{}
	buf[2] = &data.Quote{}
	buf[3] = &data.Quote{}
	buf[4] = &data.Quote{}

	ret := splitByBatch(buf[0:5], 2)

	assert.Equal(t, len(ret), 3)
	assert.Equal(t, len(ret[0]), 2)
	assert.Equal(t, len(ret[1]), 2)
	assert.Equal(t, len(ret[2]), 1)
}
