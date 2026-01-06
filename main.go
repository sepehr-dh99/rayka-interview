package main

import (
	"encoding/json"
	"fmt"
	"sort"
)

type Event struct {
	UserID string                 `json:"user_id"`
	TS     int64                  `json:"ts"`
	Type   string                 `json:"type"`
	Meta   map[string]interface{} `json:"meta"`
}

type Session struct {
	UserID  string                 `json:"user_id"`
	StartTS int64                  `json:"start_ts"`
	EndTS   int64                  `json:"end_ts"`
	Types   []string               `json:"types"`
	Meta    map[string]interface{} `json:"meta"`
}

const sessionGapThreshold = 600 // seconds

func mergeUserEvents(events []Event) []Session {
	if len(events) == 0 {
		return []Session{}
	}

	// Sort by user_id and timestamp without copying
	sortedEvents := make([]Event, len(events))
	copy(sortedEvents, events)
	sort.Slice(sortedEvents, func(i, j int) bool {
		if sortedEvents[i].UserID != sortedEvents[j].UserID {
			return sortedEvents[i].UserID < sortedEvents[j].UserID
		}
		return sortedEvents[i].TS < sortedEvents[j].TS
	})

	sessions := make([]Session, 0, len(events)/2) // Estimate: ~2 events per session

	for i := 0; i < len(sortedEvents); {
		i = processUserSessions(sortedEvents, i, &sessions)
	}

	// Sort sessions by start_ts
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].StartTS < sessions[j].StartTS
	})

	return sessions
}

// processUserSessions processes all sessions for a single user and returns the next index
func processUserSessions(events []Event, startIdx int, sessions *[]Session) int {
	userID := events[startIdx].UserID
	i := startIdx

	for i < len(events) && events[i].UserID == userID {
		session := buildSession(events, &i, userID)
		*sessions = append(*sessions, session)
	}

	return i
}

// buildSession builds a single session from consecutive events
func buildSession(events []Event, idx *int, userID string) Session {
	firstEvent := events[*idx]

	session := Session{
		UserID:  userID,
		StartTS: firstEvent.TS,
		EndTS:   firstEvent.TS,
		Types:   make([]string, 0, 8), // Pre-allocate reasonable capacity
		Meta:    cloneMap(firstEvent.Meta),
	}

	prevType := firstEvent.Type
	session.Types = append(session.Types, prevType)
	*idx++

	// Add subsequent events within session gap
	for *idx < len(events) && events[*idx].UserID == userID {
		if events[*idx].TS-events[*idx-1].TS > sessionGapThreshold {
			break
		}

		currentEvent := events[*idx]
		session.EndTS = currentEvent.TS

		// Only add type if different from previous
		if currentEvent.Type != prevType {
			session.Types = append(session.Types, currentEvent.Type)
			prevType = currentEvent.Type
		}

		// Merge metadata in-place
		mergeMaps(session.Meta, currentEvent.Meta)
		*idx++
	}

	return session
}

// mergeMaps recursively merges src into dst, keeping dst values on conflict
func mergeMaps(dst, src map[string]interface{}) {
	for key, srcVal := range src {
		if dstVal, exists := dst[key]; exists {
			// Both are maps - merge recursively
			if dstMap, dstIsMap := dstVal.(map[string]interface{}); dstIsMap {
				if srcMap, srcIsMap := srcVal.(map[string]interface{}); srcIsMap {
					mergeMaps(dstMap, srcMap)
					continue
				}
			}
			// Conflict: keep dst value (earliest)
			continue
		}

		// New key - add it (deep copy if map)
		if srcMap, ok := srcVal.(map[string]interface{}); ok {
			dst[key] = cloneMap(srcMap)
		} else {
			dst[key] = srcVal
		}
	}
}

// cloneMap creates a deep copy of a map
func cloneMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return make(map[string]interface{})
	}

	dst := make(map[string]interface{}, len(src))
	for key, val := range src {
		if valMap, ok := val.(map[string]interface{}); ok {
			dst[key] = cloneMap(valMap)
		} else {
			dst[key] = val
		}
	}
	return dst
}

func main() {
	events := []Event{
		{UserID: "u1", TS: 1000, Type: "click", Meta: map[string]interface{}{"page": "/"}},
		{UserID: "u1", TS: 1500, Type: "click", Meta: map[string]interface{}{"page": "/home"}},
		{UserID: "u1", TS: 1600, Type: "scroll", Meta: map[string]interface{}{"depth": 100}},
		{UserID: "u1", TS: 1700, Type: "scroll", Meta: map[string]interface{}{"depth": 200}},
		{UserID: "u1", TS: 2200, Type: "click", Meta: map[string]interface{}{"page": "/about"}},
		{UserID: "u2", TS: 1200, Type: "view", Meta: map[string]interface{}{"item": "A"}},
		{UserID: "u2", TS: 1300, Type: "view", Meta: map[string]interface{}{"item": "B"}},
	}

	sessions := mergeUserEvents(events)

	output, _ := json.MarshalIndent(sessions, "", "  ")
	fmt.Println(string(output))
}
