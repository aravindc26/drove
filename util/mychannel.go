package util

import "sync"

// https://go101.org/article/channel-closing.html
type MyChannel struct {
	C    chan bool
	once sync.Once
}

func NewMyChannel() *MyChannel {
	return &MyChannel{C: make(chan bool)}
}

func (mc *MyChannel) SafeClose() {
	mc.once.Do(func() {
		close(mc.C)
	})
}
