# User Events Session Merger

Merges user events into sessions based on temporal proximity and user activity.

## Overview

Groups user events into sessions where consecutive events from the same user are at most 10 minutes apart. Sessions include deduplicated event types and deeply merged metadata.

## Usage

```go
events := []Event{
    {UserID: "u1", TS: 1000, Type: "click", Meta: map[string]interface{}{"page": "/"}},
    {UserID: "u1", TS: 1500, Type: "scroll", Meta: map[string]interface{}{"depth": 100}},
    {UserID: "u1", TS: 2200, Type: "click", Meta: map[string]interface{}{"page": "/about"}},
}

sessions := mergeUserEvents(events)
// Returns 2 sessions: [1000-1500] and [2200-2200]
```

## Rules

- **Session Gap**: Events > 600 seconds apart start new sessions
- **Type Deduplication**: Consecutive duplicate types removed (`[click, click, scroll]` → `[click, scroll]`)
- **Metadata Merging**:
  - Nested maps merged recursively
  - Conflicts resolved by keeping earliest value
- **Output Sorting**: Sessions sorted by `start_ts` across all users

## Example Output

```json
[
  {
    "user_id": "u1",
    "start_ts": 1000,
    "end_ts": 1600,
    "types": ["click", "scroll"],
    "meta": { "page": "/", "depth": 100 }
  }
]
```

## Performance

- Time: `O(n log n)` - sorting dominates
- Space: `O(n)` - output sessions
- Optimized with pre-allocated slices and in-place merging
