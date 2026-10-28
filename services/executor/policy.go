package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// What D4 lets the executor repeat. A new attempt needs a positive claim that
// repeating is safe, from the source for its cause: the instrument's Retryable
// for a failure, the device's settings for a hang or a lost read. Which
// operations are safe to repeat on real equipment is a guess, so the devices
// are settings (docker-compose.yml), not code.
type Policy struct {
	RepeatSafe       []string // device id prefixes whose operation may be repeated after a hang
	RereadOnDataLoss []string // device id prefixes that read again when a read's data is lost
	MaxAttempts      int      // per step, all causes together
}

// defaultPolicy is D4's: only the plate reader repeats a hang (a hung
// incubation would incubate the plate again), and it re-reads lost data.
var defaultPolicy = Policy{RepeatSafe: []string{"plate-reader"}, RereadOnDataLoss: []string{"plate-reader"}, MaxAttempts: 3}

// PolicyFromEnv reads REPEAT_SAFE and REREAD_ON_DATA_LOSS (comma-separated
// device id prefixes; empty means none) and MAX_ATTEMPTS. Unset means D4's default.
func PolicyFromEnv(lookup func(string) (string, bool)) (Policy, error) {
	p := defaultPolicy
	if v, ok := lookup("REPEAT_SAFE"); ok {
		p.RepeatSafe = prefixes(v)
	}
	if v, ok := lookup("REREAD_ON_DATA_LOSS"); ok {
		p.RereadOnDataLoss = prefixes(v)
	}
	if v, ok := lookup("MAX_ATTEMPTS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Policy{}, fmt.Errorf("MAX_ATTEMPTS=%q: want a whole number of at least 1", v)
		}
		p.MaxAttempts = n
	}
	return p, nil
}

func prefixes(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func matches(device string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(device, p) {
			return true
		}
	}
	return false
}

// next is what to do with the step now: a new attempt, giving up with the
// reason, or nothing (nil) while an attempt is in flight or the evidence can
// still change. Only the latest attempt is judged; an earlier one still open
// (a lost read) stays open beside it (D3). After a cancel there is no new
// attempt, and the cancel is named as the reason only if there would have been
// one: a failure that was final anyway stays the step's own (D5).
func (p Policy) next(runID, name, device string, s Step) Fact {
	if s.Decided > 0 || s.GaveUp != "" {
		return nil
	}
	if s.Cancelled {
		if s.Phase() != PhaseEnded {
			return nil
		}
		if why, ok := p.judge(device, s, s.shown()); ok && why != "" {
			return GaveUp{Reason: "not repeated: " + why}
		}
		return GaveUp{Reason: CancelledReason}
	}
	why, ok := p.judge(device, s, s.current())
	switch {
	case !ok:
		return nil
	case why == "":
		n := len(s.Attempts)
		return Dispatched{JobID: protocol.JobID(runID, name, n+1), Attempt: n + 1}
	case s.Phase() != PhaseEnded:
		return nil // an earlier attempt is still open, so the step cannot fail yet
	}
	return GaveUp{Reason: "not repeated: " + why}
}

// judge says why attempt a may not be repeated, or "" if it may. ok is false
// when a has not ended in a way the policy acts on: in flight, two outcomes
// reported, or a lost read on a device that does not re-read.
func (p Policy) judge(device string, s Step, a *Attempt) (why string, ok bool) {
	if a == nil || a.Conflict != "" {
		return "", false
	}
	n := len(s.Attempts)
	switch {
	case a.Outcome == OutcomeFailed:
		if !a.Retryable {
			why = "the instrument did not mark the failure retryable"
		}
	case a.Outcome == OutcomeHung:
		if !matches(device, p.RepeatSafe) {
			why = device + " is not safe to repeat"
		}
	case s.ProducesData && a.Outcome == OutcomeCompleted && a.DataLost && !a.verified():
		if !matches(device, p.RereadOnDataLoss) {
			return "", false // the lost read stays open: its upload may still land
		}
	default:
		return "", false
	}
	if why == "" && n >= p.MaxAttempts {
		why = fmt.Sprintf("%d attempts made", n)
	}
	return why, true
}
