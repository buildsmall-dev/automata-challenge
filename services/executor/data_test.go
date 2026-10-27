package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
)

// testStorage is the compose object store (docker compose up -d storage), in a
// bucket of its own.
func testStorage(t *testing.T) *storage.Storage {
	t.Helper()
	st, err := storage.New("http://localhost:8343", "lab-data-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EnsureBucket(context.Background()); err != nil {
		t.Skipf("storage not reachable (%v); run: docker compose up -d storage", err)
	}
	return st
}

func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// readDone puts a run's read_plate where the ledger says the read completed and
// measured want, and returns its job and the ref its data belongs at.
func (h *harness) readDone(want []byte) (RunView, string, string) {
	h.t.Helper()
	rv := newRun(h.t, h.e)
	job := h.dispatch(rv, "read_plate")
	h.claim(job)
	ev := finished(job, time.Now().UTC())
	ev.SHA = digest(want)
	h.event("plate-reader-1", ev)
	h.waitStep(rv.ID, "read_plate", StepRunning)
	return rv, job, h.e.storage.Ref(protocol.DataKey(job))
}

func (h *harness) stored(job, ref string) {
	h.t.Helper()
	h.event("plate-reader-1", protocol.Event{Kind: protocol.KindStored, JobID: job, Ref: ref})
}

func (h *harness) put(job string, data []byte) {
	h.t.Helper()
	if _, err := h.e.storage.Put(context.Background(), protocol.DataKey(job), data); err != nil {
		h.t.Fatal(err)
	}
}

func TestStoredDataThatMatchesCompletesTheRead(t *testing.T) {
	h := newHarness(t, testStorage(t))
	data := []byte("well,value\nA1,0.42\n")
	rv, job, ref := h.readDone(data)
	h.put(job, data)
	h.stored(job, ref)
	sv := h.waitStep(rv.ID, "read_plate", StepCompleted)
	if sv.DataRef == nil || *sv.DataRef != ref {
		t.Errorf("data_ref = %v, want %s", sv.DataRef, ref)
	}
}

// The driver says it stored the data, but what storage holds is not what was
// measured: the read is not completed, and the record says why.
func TestStoredDataThatDiffersIsNotCompleted(t *testing.T) {
	h := newHarness(t, testStorage(t))
	rv, job, ref := h.readDone([]byte("measured"))
	h.put(job, []byte("something else"))
	h.stored(job, ref)
	deadline := time.Now().Add(5 * time.Second)
	for {
		sv := stepView(t, h.e, rv.ID, "read_plate")
		if sv.Error != nil {
			if sv.Status != StepRunning || sv.DataRef != nil || !strings.Contains(*sv.Error, "does not match") {
				t.Errorf("step = %s, data_ref %v, error %q", sv.Status, sv.DataRef, *sv.Error)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("mismatch never recorded: %+v", sv)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stored arrives before the object can be read (or storage is down): no claim
// yet, and the event is checked again later rather than dropped.
func TestStoredIsCheckedAgainUntilStorageAnswers(t *testing.T) {
	h := newHarness(t, testStorage(t))
	data := []byte("late")
	rv, job, ref := h.readDone(data)
	h.stored(job, ref)
	time.Sleep(500 * time.Millisecond)
	if sv := stepView(t, h.e, rv.ID, "read_plate"); sv.Status != StepRunning || sv.Error != nil {
		t.Fatalf("with no object: %s, error %v; want running with no error", sv.Status, sv.Error)
	}
	h.put(job, data)
	h.waitStep(rv.ID, "read_plate", StepCompleted)
}

// The executor only trusts the place the data belongs; it is not told where to look.
func TestStoredElsewhereIsDropped(t *testing.T) {
	h := newHarness(t, testStorage(t))
	data := []byte("x")
	rv, job, _ := h.readDone(data)
	h.put(job, data)
	h.stored(job, "s3://other-bucket/"+protocol.DataKey(job))
	h.waitDrained()
	if sv := stepView(t, h.e, rv.ID, "read_plate"); sv.Status != StepRunning || sv.DataRef != nil {
		t.Errorf("after a stored event with a foreign ref: %s, data_ref %v", sv.Status, sv.DataRef)
	}
}

// A driver reports a read's data lost, but an upload it sent before it died may
// still land (D3). The run must not be finished as failed while that is
// possible; when the object does appear, the executor finds it itself (no
// driver is left to report it) and the read and the run complete.
func TestLostDataKeepsTheRunOpenUntilStorageSettlesIt(t *testing.T) {
	recheck(t, 200*time.Millisecond)
	h := newHarness(t, testStorage(t))
	h.drivers()
	pr := newFakeDriver(t, h.nc, "plate-reader-1").serve()
	data := []byte("measured, uploaded late")
	pr.sha["read_plate"], pr.lose["read_plate"] = digest(data), true
	rv := h.startRun(testWorkflow)

	deadline := time.Now().Add(5 * time.Second)
	for {
		sv := stepView(t, h.e, rv.ID, "read_plate")
		if sv.Error != nil && strings.Contains(*sv.Error, "lost") {
			if sv.Status != StepRunning {
				t.Fatalf("read reported lost is %s, want running (unresolved)", sv.Status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("data_lost never recorded: %+v", sv)
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(time.Second)
	if got, _ := h.e.GetRun(context.Background(), rv.ID); got.Status != RunRunning {
		t.Fatalf("run finished as %s while the read's outcome is uncertain", got.Status)
	}

	h.put(protocol.JobID(rv.ID, "read_plate", 1), data) // the late upload lands
	done := h.waitRun(rv.ID, RunCompleted, 5*time.Second)
	if s := done.Steps[1]; s.Status != StepCompleted || s.DataRef == nil || s.Error != nil {
		t.Errorf("read after the late upload: %+v", s)
	}
}

func recheck(t *testing.T, every time.Duration) {
	old := recheckEvery
	recheckEvery = every
	t.Cleanup(func() { recheckEvery = old })
}

// The late upload of a lost read is looked for while its re-read is in flight
// too: here the re-read is claimed and never starts, and attempt 1's object,
// when it lands, completes the step and the run (D3, D4).
func TestLostReadIsFoundWhileItsReReadIsStuck(t *testing.T) {
	recheck(t, 200*time.Millisecond)
	h := newHarness(t, testStorage(t))
	h.drivers()
	pr := newFakeDriver(t, h.nc, "plate-reader-1").serve()
	data := []byte("measured, uploaded late " + randomHex(4))
	rv := h.startRun(testWorkflow)
	first, second := protocol.JobID(rv.ID, "read_plate", 1), protocol.JobID(rv.ID, "read_plate", 2)
	pr.mu.Lock()
	pr.sha["read_plate"], pr.lose[first], pr.stall[second] = digest(data), true, true
	pr.mu.Unlock()

	for deadline := time.Now().Add(5 * time.Second); phaseOf(t, h.e, rv.ID, "read_plate") != PhaseClaimed; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("re-read never claimed: read_plate is %s", phaseOf(t, h.e, rv.ID, "read_plate"))
		}
	}
	h.put(first, data) // attempt 1's upload lands while attempt 2 is stuck
	done := h.waitRun(rv.ID, RunCompleted, 5*time.Second)
	if s := done.Steps[1]; s.DataRef == nil || !strings.HasSuffix(*s.DataRef, protocol.DataKey(first)) {
		t.Errorf("read_plate: %+v, want completed from attempt 1's object", s)
	}
}
