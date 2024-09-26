package channel

import (
	"container/list"
)

func NoBlock(chIn chan interface{}) (chOut chan interface{}) {
	chOut = make(chan interface{})
	go func() {
		elements := list.New()
		for {
			var element interface{}
			var chOutIn chan interface{}
			front := elements.Front()
			if front != nil {
				element = front.Value
				chOutIn = chOut
			}
			select {
			case element, ok := <-chIn:
				if !ok {
					close(chOut)
					return
				}
				elements.PushBack(element)
			case chOutIn <- element:
				elements.Remove(front)
			}
		}
	}()
	return
}