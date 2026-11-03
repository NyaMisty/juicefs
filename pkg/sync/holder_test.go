/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sync

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

func waitForCounter(t *testing.T, counter *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if counter.Load() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("counter is %d, want %d", counter.Load(), want)
}

func muHolderAvailable() bool {
	acquired := make(chan struct{})
	go func() {
		muHolder.Lock()
		muHolder.Unlock()
		close(acquired)
	}()
	select {
	case <-acquired:
		return true
	case <-time.After(time.Second):
		return false
	}
}

// TestFetchTaskDoesNotHoldLockWhileWaiting is a regression test for a deadlock
// where fetchTask kept muHolder locked while blocking on an empty task channel.
// One worker waiting for a task would then block every other worker from
// acquiring the lock or consuming large-file holders.
func TestFetchTaskDoesNotHoldLockWhileWaiting(t *testing.T) {
	muHolder.Lock()
	holders = nil
	muHolder.Unlock()
	workersWaitingLarge.Store(0)
	workersWaitingQueue.Store(0)

	tasks := make(chan object.Object, 2) // kept empty, buffered only for release
	first := make(chan object.Object, 1)
	second := make(chan object.Object, 1)
	h := &holder{done: make(chan struct{})}

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		close(h.done)
		for i := 0; i < 2; i++ {
			select {
			case tasks <- nil:
			default:
			}
		}
	}
	var addDone chan struct{}
	defer func() {
		// Best-effort release so a failed assertion cannot leave workers stuck.
		release()
		if addDone != nil {
			select {
			case <-addDone:
			case <-time.After(time.Second):
			}
		}
		muHolder.Lock()
		holders = nil
		muHolder.Unlock()
	}()

	// The first worker blocks on the empty task channel.
	go func() {
		obj, done := fetchTask(tasks, 0)
		done()
		first <- obj
	}()
	waitForCounter(t, &workersWaitingQueue, 1)

	// Adding a holder must not be blocked by the worker waiting for a task.
	addDone = make(chan struct{})
	go func() {
		muHolder.Lock()
		holders = append(holders, h)
		muHolder.Unlock()
		close(addDone)
	}()
	select {
	case <-addDone:
	case <-time.After(2 * time.Second):
		t.Fatal("cannot add a holder while a worker waits for a task")
	}

	// A second worker must still be able to pop the holder and park on it
	// even though the first worker is blocked waiting for a task.
	go func() {
		obj, done := fetchTask(tasks, 0)
		done()
		second <- obj
	}()
	waitForCounter(t, &workersWaitingLarge, 1)

	if !muHolderAvailable() {
		t.Error("muHolder is held while workers are blocked")
	}

	release()
	for _, ch := range []chan object.Object{first, second} {
		select {
		case obj := <-ch:
			if obj != nil {
				t.Errorf("fetchTask returned %v, want nil", obj)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("fetchTask did not return after release")
		}
	}

	if n := workersWaitingLarge.Load(); n != 0 {
		t.Errorf("workersWaitingLarge = %d, want 0", n)
	}
	if n := workersWaitingQueue.Load(); n != 0 {
		t.Errorf("workersWaitingQueue = %d, want 0", n)
	}
}
