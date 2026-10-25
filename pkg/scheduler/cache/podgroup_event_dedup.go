/*
Copyright 2026 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
)

// podGroupEventKey identifies a PodGroup event by the fields that the issue
// treats as the identity of a diagnostic.
type podGroupEventKey struct {
	eventType string
	reason    string
	message   string
}

// podGroupEventDeduper suppresses identical PodGroup events across scheduling
// sessions. State is keyed by PodGroup UID and stores the set of
// (type, reason, message) values last recorded for that PodGroup, so distinct
// plugin diagnostics are kept independently. Entries are cleared when the
// PodGroup recovers or is deleted so the same failure can be reported again.
type podGroupEventDeduper struct {
	mu   sync.Mutex
	last map[types.UID]map[podGroupEventKey]struct{}
}

// shouldRecord reports whether the event should be sent and remembers it.
// An empty UID bypasses deduplication so objects without one keep prior behavior.
func (d *podGroupEventDeduper) shouldRecord(uid types.UID, eventType, reason, message string) bool {
	if uid == "" {
		return true
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.last == nil {
		d.last = make(map[types.UID]map[podGroupEventKey]struct{})
	}
	keys, found := d.last[uid]
	if !found {
		keys = make(map[podGroupEventKey]struct{})
		d.last[uid] = keys
	}

	key := podGroupEventKey{eventType: eventType, reason: reason, message: message}
	if _, seen := keys[key]; seen {
		return false
	}
	keys[key] = struct{}{}
	return true
}

// clear drops remembered events for a PodGroup so a later identical failure
// can be recorded again after recovery.
func (d *podGroupEventDeduper) clear(uid types.UID) {
	if uid == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.last, uid)
}

// forget drops remembered events when a PodGroup is deleted.
func (d *podGroupEventDeduper) forget(uid types.UID) {
	d.clear(uid)
}
