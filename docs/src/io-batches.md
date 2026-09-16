# Batch representation

A batch is an ordered sequence of logical ticks. Internally it is stored as a flat list of **segments**, and each segment holds its updates in one of three layouts. Every public operation constructs, reads, or transforms a batch through its logical ticks, so the choice of layout never changes how many ticks a batch contains, their order, or which updates are simultaneous.

This page explains that representation for both directions. `InputBatch` and `OutputBatch` are separate public types, but they share the storage and iteration code in `src/core/batch.rs`.

## Scope of the representation

Sources and runtimes produce data in different native shapes. An MQTT message carries one value, a channel source joins values that arrived together, a file yields table rows, and the dataflow runtime completes one output row per model step. The batch representation lets each producer keep its native shape while every consumer sees the same ordered ticks.

| The representation is responsible for | It is not responsible for |
|---|---|
| Recording which updates are simultaneous and in which order ticks occur. | Choosing sources or destinations; see [input](input-architecture.md) and [output](output.md) architecture. |
| Storing single updates, simultaneous updates, and fixed-layout rows without converting between them. | Deciding when to collect ticks into a batch; input windows and output coalescing do that. |
| Validating tick and layout shape when a batch is constructed. | Checking that a variable belongs to a model or a destination; runtimes and writers do that. |
| Providing borrowed and owned iteration over ticks and updates. | Assigning model time: a batch boundary adds no model step. |

## Logical and physical units

| Entity | Responsibility |
|---|---|
| **update** (`InputUpdate`, `OutputUpdate`) | One variable taking one value. |
| **logical tick** | A non-empty set of updates evaluated or published together as one model step. A tick names each variable at most once. Its **width** is its number of updates. |
| **batch** (`InputBatch`, `OutputBatch`) | An ordered sequence of logical ticks, delivered together. |
| **segment** (`InputSegment`, `OutputSegment`) | A physical run of consecutive ticks stored in one layout. A segment is never itself a batch. |
| **storage** (`Storage`) | Either one segment or a flat list of segments; batches never contain other batches. |
| **tick and update views** (`Tick`, `UpdateRef`) | Borrowed, layout-independent access to one tick and to one update. |

Both update types are aliases of one shared definition:

```rust
{{#include ../../src/core/batch.rs:batch_update}}
```

A tick has no owned type of its own: owned code represents one as `Vec<Update<V>>`, and borrowed code uses the `Tick` view described under [Iterating a batch](#iterating-a-batch).

## A running example

The rest of this page uses one batch of four ticks:

| Tick | Updates | Width |
|---:|---|---:|
| 0 | `x = 1` | 1 |
| 1 | `x = 2`, `y = 20` | 2 |
| 2 | `x = 3`, `y = 30` | 2 |
| 3 | `x = 4`, `y = 40` | 2 |

It contains four ticks and seven updates. The example builds ticks 0 and 1 from listed ticks and ticks 2 and 3 from fixed-layout rows, then concatenates the two batches:

```rust
{{#include ../../tests/docs_examples.rs:batch_representation}}
```

`len` returns the update count, while `is_empty` asks whether the batch has any ticks. The example uses `OutputBatch` because packed-row construction is public only for output; `InputBatch::packed_rows` is available inside the crate to file and in-memory sources.

The resulting batch stores each part of the example in the layout it was built with:

{{#include assets/batch-segments-ticks.svg}}

**Reading rule.** Horizontal position is logical tick order, and a cell below a tick mark belongs to that tick. The `PackedRows` segment stores ticks 2 and 3 in one contiguous value buffer; each row of its layout is one tick. The bracket shows that all three segments sit side by side in one list, with no batch nested inside another.

## Segments

Each segment kind groups its values into ticks differently:

| Segment | Stores | Ticks represented | Typical producers |
|---|---|---|---|
| `SingletonTicks` | a list of updates | one width-one tick per update | MQTT, Redis, and ROS input (`InputBatch::update`); asynchronous, distributed, and MSTLO output |
| `Tick` | a list of updates | exactly one tick containing all of them | channel-source joins (`InputBatch::tick`); semi-synchronous output rows |
| `PackedRows` | a layout and a flat value buffer in row-major order | one tick per row, `values.len() / layout.len()` in total | file and in-memory row input; dataflow output |

`SingletonTicks` and `Tick` hold the same data, a list of updates, and differ only in how that list divides into ticks. `PackedRows` stores each variable name once, in its layout, instead of once per value. The input definition, which is internal to the crate, documents each kind:

```rust
{{#include ../../src/core/input.rs:input_segment}}
```

The output segment has the same three kinds. Its packed layout is a `ValidatedLayout`, which is checked once and shared by reference between batches:

```rust
{{#include ../../src/core/output.rs:output_segment}}
```

Construction validates each segment:

- A `Tick` must be non-empty and name each variable at most once.
- A `PackedRows` layout must be non-empty and name each variable at most once, and the value count must be a whole number of rows.
- A valid output `PackedRows` segment contains at least one row; `OutputBatch::empty()` represents empty output. An input `PackedRows` segment may have no rows: the crate-private `InputBatch::packed_rows` uses one to represent an empty batch with a known layout.
- `SingletonTicks` has no shape rule: each update is a complete tick. A variable may therefore appear in several successive singleton ticks.

## Storage

A batch owns one `Storage` value. The single-segment case avoids allocating a list, which is the common shape for one message or one row:

```rust
{{#include ../../src/core/batch.rs:batch_storage}}
```

```rust
{{#include ../../src/core/input.rs:input_batch}}
```

Constructors choose the initial layout:

| Constructor | Resulting storage |
|---|---|
| `update(variable, value)` | one `SingletonTicks` segment with one update |
| `tick(updates)` | one `Tick` segment |
| `from_ticks(ticks)` | consecutive width-one ticks grouped into `SingletonTicks`; every wider tick as its own `Tick` segment |
| `OutputBatch::packed_rows(layout, values)` | one `PackedRows` segment |
| `empty()` | one empty `SingletonTicks` segment; output never represents emptiness with a packed segment |

When batches are combined, segments without ticks are dropped and the remaining segments keep their order. The two directions then differ:

- **Input** keeps adjacent segments separate. `InputBatch::concat` moves whole segments into one list. The input-window collector also joins segments without merging them, although it may split a segment between ticks to meet a window limit.
- **Output** merges adjacent `SingletonTicks` segments, and adjacent `PackedRows` segments with the same layout. `OutputBatch::concat` and the crate-private `append` used by output coalescing both apply these rules.

Neither direction merges two ticks. Two batches with the same ticks can still differ in storage, and because batch equality compares storage, such batches do not compare equal.

## Iterating a batch

A batch can be read in three ways. They differ in whether tick boundaries survive and whether packed rows are expanded:

| Iterator | Obtained from | Yields | Tick boundaries |
|---|---|---|---|
| borrowed ticks | `batch.ticks()`, or `for tick in &output_batch` | `Tick<'_, V>` | kept |
| borrowed updates | `batch.updates()` | `UpdateRef<'_, V>` | lost |
| owned ticks | `output_batch.into_ticks()`, or `for tick in output_batch` | `Vec<Update<V>>` | kept |

Their costs differ:

- **Borrowed ticks** (`InputTicks`, `OutputTicks`) allocate nothing. A packed row becomes a tick that refers into the value buffer.
- **Borrowed updates** (`InputUpdates`, `OutputUpdates`) allocate nothing, and list the updates of every tick one after another. A packed value takes its variable name from its layout column.
- **Owned ticks** (`OwnedInputTicks`, `OwnedOutputTicks`) consume the batch and build one `Vec` per tick. This is the only place packed rows are expanded.

All three yield items in logical order: segment order, then position within a segment. Each is an `ExactSizeIterator` whose length comes from the batch's tick or update count, and none is double-ended.

A borrowed tick is a small `Copy` handle. It refers either to a slice of updates or to one packed row and its layout, and `UpdateRef` borrows one variable and value from either:

```rust
{{#include ../../src/core/batch.rs:batch_tick}}
```

`tick.updates()` (or `tick.iter()`) yields the tick's `UpdateRef`s, `tick.len()` is its width, and `tick.to_updates()` clones it into a `Vec<Update<V>>`.

Iterating borrowed ticks gives the same result whichever segment stores a tick:

```rust
{{#include ../../tests/docs_examples.rs:batch_iterate_ticks}}
```

`updates()` suits code that needs every value but not their grouping, such as checking which variables a batch names. It cannot show which values were simultaneous, so evaluation code iterates ticks instead:

```rust
{{#include ../../tests/docs_examples.rs:batch_iterate_updates}}
```

Consuming a batch yields owned ticks; tick 2, stored as a packed row, becomes an ordinary list of updates:

```rust
{{#include ../../tests/docs_examples.rs:batch_owned_ticks}}
```

The two directions expose iteration differently. `OutputBatch` implements `IntoIterator` by value (owned ticks) and by reference (borrowed ticks), and `into_ticks` is public. `InputBatch` publicly offers the borrowed `ticks()` and `updates()` views:

```rust
{{#include ../../tests/docs_examples.rs:input_batch_iteration}}
```

Its `into_ticks` is crate-private. Runtimes expand input ticks at their own boundary through it: `borrowed_tick_stream` turns the semi-synchronous runtime's input into one item per tick, `into_tick_stream` does the same for the distributed runtime, and the asynchronous input fanout and reconfigurable semi-synchronous runtime call `into_ticks` directly.

The dataflow runtime adapter does not iterate ticks when a batch is a single packed segment. It reads the layout and value buffer through the crate-private `packed_rows_segment` and evaluates each row in place, falling back to `ticks()` for every other shape; see [Runtime adapter](architecture/dataflow/runtime-adapter.md).

## Operations that preserve ticks

Most operations change storage, if anything, but leave the ticks alone. One input policy deliberately creates a new tick:

| Operation | Ticks | Storage |
|---|---|---|
| `concat` | preserved and ordered: first batch, then second | combined as described under [Storage](#storage) |
| `select_variables` | each tick keeps only the selected variables; a tick left with none is removed | a packed segment stays packed with the selected columns; a segment with no selected column is removed |
| `map_values`, `map_update_values`, `try_map_values`, `try_map_update_values` (output also `map`, `try_map`) | preserved | preserved |
| input window, `InputPolicy::Batch` | preserved | may be split between ticks to meet a limit; a tick is never split |
| input window, `InputPolicy::WindowToStep` | a window of ticks is reduced to **one new tick** | new `Tick` segment |
| output coalescing | preserved, including successive ticks for the same variable | merged as described under [Storage](#storage) |

Selecting a variable removes only the ticks that would become empty:

```rust
{{#include ../../tests/docs_examples.rs:batch_select_variables}}
```

The [input architecture](input-architecture.md#collecting-observations-into-input-windows) describes windows, and the [output architecture](output.md) describes destination selection and coalescing.

## Implementation mapping

- Shared update, storage, borrowed tick and update iterators, and owned tick iterator: `Update`, `Storage`, `normalize`, `Tick`, `UpdateRef`, `Ticks`, `Updates`, and `OwnedTicks` in `src/core/batch.rs`.
- Input segments, validation, construction, selection, mapping, and tick streams: `InputSegment`, `InputBatch`, `into_tick_stream`, and `borrowed_tick_stream` in `src/core/input.rs`.
- Output segments, validated layouts, merging, and `IntoIterator`: `OutputSegment`, `OutputBatch`, and `storage_from_segments` in `src/core/output.rs`; `ValidatedLayout` in `src/core/layout.rs`.
- Packed-row sources: `src/io/file/input_stream.rs` and `src/io/map/input_stream.rs`.
- Input windows, including their segment-preserving batching and tick-creating reduction: `drive_window` in `src/io/aggregation.rs`. Output coalescing: `src/io/output/delivery.rs`.
- Runtime producers and consumers: `consume_singleton_streams` and `consume_row_streams` in `src/runtime/output_utils.rs`; the packed-row path in `src/runtime/dataflow.rs`.
- Examples on this page: `tests/docs_examples.rs`.
