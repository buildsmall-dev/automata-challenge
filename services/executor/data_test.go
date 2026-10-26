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
