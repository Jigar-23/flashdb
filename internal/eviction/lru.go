package eviction

import "sync"

// Node represents a node in the doubly linked list
type Node struct {
	key   string
	prev  *Node
	next  *Node
}

// LRU implements an O(1) thread-safe Least Recently Used cache tracker
type LRU struct {
	mu       sync.Mutex
	items    map[string]*Node
	head     *Node // Most recently used
	tail     *Node // Least recently used
	capacity int64 // Max entries (or 0 for unbounded)
}

// NewLRU creates a new LRU tracker
func NewLRU(capacity int64) *LRU {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.prev = head
	return &LRU{
		items:    make(map[string]*Node),
		head:     head,
		tail:     tail,
		capacity: capacity,
	}
}

// Access marks a key as accessed (moves to head of list)
func (l *LRU) Access(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	node, exists := l.items[key]
	if exists {
		l.removeNode(node)
		l.insertHead(node)
		return
	}

	newNode := &Node{key: key}
	l.items[key] = newNode
	l.insertHead(newNode)
}

// Remove removes a key from the tracker
func (l *LRU) Remove(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	node, exists := l.items[key]
	if !exists {
		return
	}
	l.removeNode(node)
	delete(l.items, key)
}

// Evict returns the least recently used key and removes it
func (l *LRU) Evict() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.items) == 0 {
		return "", false
	}

	lruNode := l.tail.prev
	if lruNode == l.head {
		return "", false
	}

	l.removeNode(lruNode)
	delete(l.items, lruNode.key)
	return lruNode.key, true
}

func (l *LRU) removeNode(node *Node) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (l *LRU) insertHead(node *Node) {
	node.next = l.head.next
	node.prev = l.head
	l.head.next.prev = node
	l.head.next = node
}

// Len returns current number of tracked items
func (l *LRU) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}
