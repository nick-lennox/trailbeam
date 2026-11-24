package trailbeamsqs

import (
	"container/list"
	"sync"
	"time"
)

// handleMap maps the receipt handles this process received to their message IDs, so a settle span can
// carry the message ID (contract section 8). Safe for concurrent use and bounded: at most maxEntries, each
// dropped after maxAge (12 hours, the longest visibility timeout) or on a successful delete.
type handleMap struct {
	mu         sync.Mutex
	byHandle   map[string]*list.Element
	order      *list.List // oldest first: insertion order is time order
	maxEntries int
	maxAge     time.Duration
	now        func() time.Time
}

type handleEntry struct {
	handle, messageID string
	at                time.Time
}

const (
	handleMaxAge     = 12 * time.Hour
	handleMaxEntries = 100_000
)

func newHandleMap(maxEntries int, maxAge time.Duration, now func() time.Time) *handleMap {
	return &handleMap{byHandle: map[string]*list.Element{}, order: list.New(), maxEntries: maxEntries,
		maxAge: maxAge, now: now}
}

func (m *handleMap) put(handle, messageID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if e, ok := m.byHandle[handle]; ok {
		m.order.Remove(e)
	}
	m.byHandle[handle] = m.order.PushBack(&handleEntry{handle, messageID, now})
	for front := m.order.Front(); front != nil; front = m.order.Front() {
		e := front.Value.(*handleEntry)
		if m.order.Len() <= m.maxEntries && now.Sub(e.at) <= m.maxAge {
			break
		}
		m.order.Remove(front)
		delete(m.byHandle, e.handle)
	}
}

func (m *handleMap) get(handle string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.byHandle[handle]
	if !ok {
		return ""
	}
	e := el.Value.(*handleEntry)
	if m.now().Sub(e.at) > m.maxAge {
		m.order.Remove(el)
		delete(m.byHandle, handle)
		return ""
	}
	return e.messageID
}

func (m *handleMap) remove(handle string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.byHandle[handle]; ok {
		m.order.Remove(el)
		delete(m.byHandle, handle)
	}
}

func (m *handleMap) size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.order.Len()
}
