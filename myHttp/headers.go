package myHttp

import "bytes"

func byteToInt(b []byte) (c uint, ok bool) {
	l := len(b)
	if l == 0 {
		return
	}
	for i := 0; i < l; i++ {
		if b[i] < 48 || b[i] > 57 {
			return
		}
		c *= 10
		c += uint(b[i] - '0')
	}
	ok = true
	return
}

func byteToInt2(b []byte) (c int, ok bool) {
	l := len(b)
	if l == 0 {
		return
	}
	for i := 0; i < l; i++ {
		if b[i] < 48 || b[i] > 57 {
			return
		}
		c *= 10
		c += int(b[i] - '0')
	}
	ok = true
	return
}

type Headers struct{
	slices [][]byte
}

func (c *Headers) Clear() {
	c.slices = c.slices[:0]
}

func (c *Headers) Has(key []byte) bool {
	for i := 0; i < len(c.slices); i += 2 {
		if bytes.Equal(key, c.slices[i]) {
			return true
		}
	}
	return false
}

func (c *Headers) Add(key []byte, value []byte) {
	c.slices = append(c.slices, key, value)
}

func (c *Headers) GetUint(key []byte) (a uint, b bool) {
	for i := 0; i < len(c.slices); i += 2 {
		if bytes.Equal(key, c.slices[i]) {
			a, b = byteToInt(c.slices[i + 1])
			b = !b
		}
	}
	return
}

func (c *Headers) GetInt(key []byte) (a int, b bool) {
	for i := 0; i < len(c.slices); i += 2 {
		if bytes.Equal(key, c.slices[i]) {
			a, b = byteToInt2(c.slices[i + 1])
			b = !b
		}
	}
	return
}

func (c *Headers) GetString(key []byte) []byte {
	for i := 0; i < len(c.slices); i += 2 {
		if bytes.Equal(key, c.slices[i]) {
			return c.slices[i + 1]
		}
	}
	return nil
}