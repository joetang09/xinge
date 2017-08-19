package xinge

import (
	"log"
	"testing"
)

func TestXG(t *testing.T) {
	xg := NewXg("xxx", "xxx")
	msg := NewSimplyMessage(MESSAGE_TYPE_ANDROID_NOTIFICATION)
	msg.Set("title", "test")
	msg.Set("content", "test")
	data, e := xg.Push2AllDevice(msg)
	log.Println(data, e)
}
