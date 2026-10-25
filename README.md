# Lab Automation — Executor and Drivers

## Context

This is a slice of a lab automation platform: software that drives robotic lab
equipment through scientific workflows.

- **Instruments** are physical equipment: a liquid handler, an incubator, a
  plate reader. Each does **one thing at a time**. The plate reader produces a
  data file for every read.
- **A driver** runs next to each instrument and connects it to the rest of the
  system.
- **A workflow** is a DAG of steps. Each step names the device that must run it
  and the steps that have to finish first.
- **The executor** owns runs and their steps, and serves the APIs.

Workflows live in `config/workflows.yaml`. The default one:

```
fill_sample_plate ──┬──> incubate_samples ──────────────────────────┐
                    │                                               │
                    ├──> fill_reagent_plate ──> warm_reagent_plate ──┤──> combine ──> read_plate
                    │                                               │
                    └──> fill_buffer_plate ─────────────────────────┘
```

| Step | Device |
| --- | --- |
| `fill_sample_plate` | liquid handler |
| `incubate_samples` | incubator |
| `fill_reagent_plate` | liquid handler |
| `fill_buffer_plate` | liquid handler |
| `warm_reagent_plate` | incubator |
| `combine` | liquid handler |
| `read_plate` | plate reader |

## Architecture

```
   HTTP + GraphQL :5002
              │
       ┌──────▼───────┐          ┌────────────┐
       │   executor   │──────────│  Postgres  │
       └──────┬───────┘          └────────────┘
              │
     ┌────────▼─────────┐        ┌──────────────────┐
     │  NATS (JetStream │        │  object storage  │
     │   switched on)   │        │  (S3, SeaweedFS) │
     └──┬──────┬─────┬──┘        └──────────────────┘
        │      │     │
   ┌────▼─┐ ┌──▼───┐ ┌▼─────┐
   │driver│ │driver│ │driver│    each driver: your adapter + the instrument
   └──────┘ └──────┘ └──────┘
   liquid-   incubator-  plate-
   handler-1 1           reader-1
```

Everyone can reach NATS and object storage. Only the executor uses Postgres.

---

# The exercise

A scientist reads the run record in the morning and acts on it: which plates
were filled, which were read, where the data is. **The record must be true.**
That covers what the API reports for a run and its steps, and the data the
plate reader produced.

We expect three things:

1. **Make the messaging robust.** Build the communication between the executor
   and the drivers, on both ends, over NATS. The network, the processes and the
   instruments are not reliable. Use the chaos tools (see
   [Breaking things](#breaking-things)) to find out where the record stops
   being true, and make it hold.
2. **Design the GraphQL interface.** Design a GraphQL schema for the run
   record and serve it from the executor, at `/graphql` on the same port.
   Scientists read the record through it, and tools use it to start runs and
   follow them. What it exposes, and how, is yours to decide. The HTTP API
   stays as it is: our checks use it, and only it.
3. **Write the note.** `NOTES.md`, as described in
   [The note](#the-note-required). It carries as much weight as the code.

**Three hours is a good amount of time.** Spend as much as you want — just tell
us roughly how long you actually spent. We are not scoring speed. Knowing the
time is what lets us read everything else fairly.

**The check we give you is the floor.** `./scripts/acceptance.sh` runs one
workflow with nothing going wrong. We expect it to pass. It tells us very
little. What we read closely is what you did above it, and what you can tell
us about it.

You are welcome to use AI assistance. We will ask you to walk us through your
reasoning and your choices, so work in a way that lets you do that. If you use
AI, **include the conversations with your submission** (see Submitting).

## What is here

| | |
| --- | --- |
| `services/executor/service.go` | **Yours.** The executor side. Runs are created and read already; nothing moves them forward |
| `services/driver/adapter.go` | **Yours.** The driver side. Connects one instrument to the rest of the system |
| `db/init.sql` | **Yours.** A starting point, not a design |
| `services/driver/instrument/instrument.go` | **Fixed.** The instrument. Read it; do not change it |
| `services/executor/scheduler.go` | Given. A pure function that says which steps should start next |
| `services/executor/api.go`, `models.go` | Given. The HTTP API and the shape it reports. Keep it: the checks use it |
| `services/common/storage/` | Given. A small S3 client |
| `examples/` | Two small NATS programs: core request/reply, and JetStream publish and consume. Not part of the system |
| `scripts/acceptance.sh` | One run on the happy path |
| `scripts/chaos.sh` | Breaks things on purpose |

Everything except `instrument.go` is yours to change. Some things must keep
their shape, because the checks depend on them:

- `Executor` in `service.go`: `NewExecutor`, `Start`, `Close`, `CreateRun`,
  `StartRun`, `CancelRun`, `GetRun` and `ListRuns`.
- `Serve(ctx, inst, nc, st)` in `adapter.go`.
- `RunView` and `StepView` in `models.go`, their JSON names, and their status values:
  `pending`, `running`, `completed`, `failed`, `cancelled`.
- `data_ref` on a step is where its data is stored, as `s3://bucket/key`.
- The HTTP API in `api.go`: its paths and what they return.

Add any library you need (`go get <module>`), including one for GraphQL.

## The instrument

`services/driver/instrument/instrument.go` stands for the physical device. In short:

- `inst.Run(ctx, instrument.Job{...})` does the work and returns an `Outcome`
  when it ends. It returns `instrument.ErrBusy` straight away if it is already
  working. Ending `ctx` stops the waiting, not the work.
- `JobID` is yours to choose. The instrument records it.
- An `Outcome` can carry an `Error`, with `Retryable`: the instrument's own view
  of whether the same step could sensibly be run again. The liquid handler says
  `false` — a part-dispensed plate cannot safely be dispensed again. The
  incubator and the plate reader say `true`.
- The plate reader's `Outcome` carries `Data`.
- `inst.Status()` tells you what it is doing now and everything it has done.

Every physical execution goes into the instrument's ledger. The checks read
that ledger through a side channel (`curl -s localhost:7101/status | jq`,
ports 7101–7103), not through anything you build.

## NATS, briefly

NATS is a message system. Messages are published to **subjects**, such as
`lab.something.incubator-1`, and delivered to whoever subscribes to a matching
subject. **Request/reply** sends a message and waits for one answer. It fails
quickly when nobody is listening, and times out when nobody answers.

Two layers, both available here:

- **Core NATS** delivers messages only to subscribers connected at the moment
  of publication — at most once, never replayed.
  ([docs](https://docs.nats.io/nats-concepts/jetstream))
- **JetStream** adds a persistence layer on top, giving you at-least-once
  delivery — messages survive restarts and can be replayed.
  ([same page](https://docs.nats.io/nats-concepts/jetstream)) Messages are
  stored in **streams**, and read through **consumers**. A consumer
  acknowledges what it has handled; if a message isn't acknowledged in time,
  the server redelivers it.
  ([consumers](https://docs.nats.io/nats-concepts/jetstream/consumers/example_configuration))

The server runs with JetStream available. You get a connected client (`nc`)
on both sides; `jetstream.New(nc)` gives you JetStream.

Two small programs in `examples/` show the Go client for each layer. They
run on their own against the stack's NATS and are not part of the system:

```bash
go run ./examples/core_request_reply
go run ./examples/jetstream_pub_sub
```

None of this is a recommendation. Use what you think is right.

## Getting started

Prerequisites: Docker and Docker Compose, `curl`, `jq`.

```bash
docker compose up --build
```

After changing code:

```bash
docker compose up --build -d executor liquid-handler-1 incubator-1 plate-reader-1
```

If you change `db/init.sql`, the database must be recreated:
`docker compose down -v`.

```bash
curl -s localhost:5002/health    | jq
curl -s localhost:5002/workflows | jq
curl -s localhost:7103/status    | jq   # what the plate reader has done

./scripts/acceptance.sh                  # fails until you build something
```

## Breaking things

```bash
./scripts/chaos.sh kill executor
./scripts/chaos.sh disconnect <executor|liquid-handler-1|incubator-1|plate-reader-1> <secs>
./scripts/chaos.sh storage-down <secs>
./scripts/chaos.sh random --seed 7 --for 60
```

A disconnected driver's instrument carries on with what it was doing.
`random` runs one fault at a time from those three. The same seed gives the
same faults at the same times.

The instruments themselves can misbehave too. Each takes two numbers from 0 to
100, both `0` by default:

| Driver | Fails | Hangs |
| --- | --- | --- |
| `liquid-handler-1` | `LH_FAIL_PCT` | `LH_HANG_PCT` |
| `incubator-1` | `INC_FAIL_PCT` | `INC_HANG_PCT` |
| `plate-reader-1` | `PR_FAIL_PCT` | `PR_HANG_PCT` |

A failing step ends with an error. A hanging step never ends; the instrument
frees itself after `HANG_DURATION` (default `30s`) and reports nothing.
`CHAOS_SEED` makes both repeatable.

```bash
INC_HANG_PCT=30 docker compose up -d incubator-1    # set
INC_HANG_PCT=0  docker compose up -d incubator-1    # back to normal
```

Set them back to `0` before `acceptance.sh`.

## The note (required)

Write `NOTES.md`. A submission without one is incomplete, and it carries as
much weight as the code. Stop building while you still have time to write it.

Tell us:

1. **Your topology.** What goes between the executor and the drivers: the
   subjects, and any streams and consumers. Who sends what, who acknowledges
   what, and when. Where each piece of state lives. A diagram is welcome.
2. **Why this topology**, and the decisions that shaped it. For each one, what
   you chose instead of the alternatives, and why.
3. **Your GraphQL schema**, and why it is shaped that way.
4. **What you did to break it, and what happened.** Seeds, commands, what you
   saw. What you changed because of it.
5. **What is still not true**, or not proven. Where the boundary of your work
   is, and whether you drew it on purpose.
6. **How long you spent.**

### Then tell us what you thought of the exercise

What was unclear, what was annoying, what you would change.

## Submitting

Push this directory to a repository of your own on GitHub, with your
`NOTES.md`, and send us the link.

If you used AI assistance, include the conversations: export them into an
`ai/` folder in the repository, or put share links in `NOTES.md`. Leave out
anything you would not want us to read, and say that you did. They help us
read your work; they do not count against you.

Before you send it, check that:

- `NOTES.md` is committed — without it the submission is incomplete
- it says roughly how long you spent
- your AI conversations are in `ai/` or linked from `NOTES.md`, if you used AI

## API reference

### executor (port 5002)

| Method | Path | |
| --- | --- | --- |
| `GET` | `/health` | |
| `GET` | `/workflows` | What `workflows.yaml` defines now |
| `GET` | `/runs` | Every run |
| `POST` | `/runs` | Create a run. Body `{"workflow_name": "..."}`, optional |
| `GET` | `/runs/{id}` | One run and its steps |
| `POST` | `/runs/{id}/start` | Start it |
| `POST` | `/runs/{id}/cancel` | Cancel it |
| `GET` | `/runs/{id}/timeline` | When each step occupied its device |
| | `/graphql` | **Yours.** Not there yet |

### instruments (ports 7101, 7102, 7103)

| Method | Path | |
| --- | --- | --- |
| `GET` | `/status` | What the instrument is doing, and every execution it has made |

## Questions

If something is unclear, make a sensible assumption, write it down in the note,
and carry on. You can also email us.
