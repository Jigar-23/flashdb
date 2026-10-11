package engine

import (
	"time"
)

// DataType represents the supported data types in FlashDB
type DataType byte

const (
	TypeString DataType = iota
	TypeList
	TypeHash
	TypeSet
)

// Entry represents an object stored in the database
type Entry struct {
	Type         DataType
	Value        interface{}
	CreatedAt    int64 // Unix millis
	ExpiresAt    int64 // Unix millis (0 = no expiration)
	LastAccessed int64 // Unix millis
	AccessCount  int64 // For LFU
	Size         int64 // Estimated memory size in bytes
}

// NewStringEntry creates a string entry
func NewStringEntry(val string) *Entry {
	now := time.Now().UnixMilli()
	return &Entry{
		Type:         TypeString,
		Value:        val,
		CreatedAt:    now,
		ExpiresAt:    0,
		LastAccessed: now,
		AccessCount:  1,
		Size:         int64(len(val) + 64),
	}
}

// IsExpired checks if the entry has expired
func (e *Entry) IsExpired(now int64) bool {
	if e.ExpiresAt == 0 {
		return false
	}
	return now >= e.ExpiresAt
}
