package mongo

import (
	"fmt"
	"gotest.tools/assert"
	"msq.ai/data"
	"testing"
	"time"
)

func TestSplitByBatch1(t *testing.T) {

	var buf [bufferSize]interface{}

	ret := splitByBatch(buf[0:0], 10)

	assert.Equal(t, len(ret), 0)
}

func TestSplitByBatch2(t *testing.T) {

	var buf [bufferSize]interface{}

	buf[0] = &data.Quote{}

	ret := splitByBatch(buf[0:1], 10)

	assert.Equal(t, len(ret), 1)
	assert.Equal(t, len(ret[0]), 1)
}

func TestSplitByBatch3(t *testing.T) {

	var buf [bufferSize]interface{}

	buf[0] = &data.Quote{}
	buf[1] = &data.Quote{}
	buf[2] = &data.Quote{}
	buf[3] = &data.Quote{}

	ret := splitByBatch(buf[0:4], 4)

	assert.Equal(t, len(ret), 1)
	assert.Equal(t, len(ret[0]), 4)
}

func TestSplitByBatch4(t *testing.T) {

	var buf [bufferSize]interface{}

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

	var buf [bufferSize]interface{}

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

func TestSplitByBatch6(t *testing.T) {

	const batch = 1000

	var buf [bufferSize]interface{}

	for i := 0; i < bufferSize; i++ {
		buf[i] = &data.Quote{}
	}

	start := time.Now()

	ret := splitByBatch(buf[0:bufferSize], batch)

	fmt.Printf("%v\n", time.Since(start))

	assert.Equal(t, len(ret), bufferSize/batch)

	for i := 0; i < bufferSize/batch; i++ {
		assert.Equal(t, len(ret[i]), batch)
	}
}
