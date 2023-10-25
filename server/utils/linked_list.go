package utils

type Node[T comparable] struct {
	value T
	next  *Node[T]
}

func NewNode[T comparable](value T, next *Node[T]) *Node[T] {
	return &Node[T]{
		value: value,
		next:  next,
	}
}

type LinkedList[T comparable] struct {
	head     *Node[T]
	tail     *Node[T]
	circular bool
}

func NewLinkedList[T comparable](circular bool) *LinkedList[T] {
	return &LinkedList[T]{
		head:     nil,
		tail:     nil,
		circular: circular,
	}
}

func (l *LinkedList[T]) Add(value T) {
	if l.head == nil {
		l.head = NewNode(value, nil)
		return
	}

	if l.tail == nil && !l.circular {
		l.tail = NewNode(value, nil)
		l.head.next = l.tail
		return
	}

	if l.tail == nil && l.circular {
		l.tail = NewNode(value, l.head)
		l.head.next = l.tail
		return
	}

	if !l.circular {
		n := NewNode(value, nil)
		l.tail.next = n
		l.tail = n
		return
	}

	n := NewNode(value, l.head)
	l.tail.next = n
	l.tail = n
}

func (l *LinkedList[T]) Remove(value T) {
	if !l.circular {
		n := l.head

		if n == nil || (n.next == nil && n.value != value) {
			return
		} else if n.next == nil && n.value == value {
			l.head = nil
			return
		}

		for {
			if n == l.head {
				return
			}
			if n.next.value == value {
				n.next = n.next.next
				if n.next == nil {
					l.tail = n
				}
				return
			}
			n = n.next
		}
	}
}

func (l *LinkedList[T]) Contains(value T) {
	// Check if a node with given value is contained within the linked list
}

func (l *LinkedList[T]) Iter() *chan *Node[T] {
	c := make(chan *Node[T])
	go func() {
		n := l.head
		for {
			c <- n
			n = n.next
			if n == nil {
				break
			}
		}
		close(c)
	}()
	return &c
}
