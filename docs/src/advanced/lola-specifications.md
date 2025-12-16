# Lola Specifications

This section provides an in-depth guide to writing Lola specifications for runtime monitoring.

## What is Lola?

Lola (Logic of Logs and Automata) is a stream-based specification language designed for runtime monitoring. It allows you to express temporal properties over data streams in a declarative way.

### Key Concepts

- **Streams**: Sequences of values over time
- **Synchronous**: All streams progress in lock-step
- **Declarative**: Describe what to compute, not how
- **Real-time**: Evaluate as events occur

## Basic Syntax

### Stream Declarations

```lola
input x: Int           # Input stream of integers
output y: Float        # Output stream of floats
```

### Type System

Lola supports standard data types:

```lola
input int_val: Int
input float_val: Float
input bool_val: Bool
input string_val: String
```

### Arithmetic Operations

```lola
output sum := x + y
output product := x * y
output difference := x - y
output quotient := x / y
output power := x ^ 2
```

### Boolean Operations

```lola
output and_result := a && b
output or_result := a || b
output not_result := !a
```

### Comparison Operations

```lola
output equals := x == y
output not_equals := x != y
output greater := x > y
output less := x < y
output gte := x >= y
output lte := x <= y
```

## Temporal Operations

### Past References

Access previous values in a stream:

```lola
output previous := x[-1]           # Previous value
output two_ago := x[-2]            # Two steps ago
output delta := x - x[-1]          # Change from previous
```

### Future References (Offline Only)

For offline analysis, you can reference future values:

```lola
output next := x[1]                # Next value
output two_ahead := x[2]           # Two steps ahead
```

**Note**: Future references are not available in online/real-time monitoring.

### Conditional Expressions

```lola
output result := if condition then value1 else value2

output sign := if x > 0 then "positive"
               else if x < 0 then "negative"
               else "zero"
```

## Advanced Features

### Windowed Aggregations

Compute over time windows:

```lola
output moving_average := (x + x[-1] + x[-2]) / 3
output sum_last_5 := x + x[-1] + x[-2] + x[-3] + x[-4]
```

### Stateful Computations

Maintain state across time steps:

```lola
output counter := if x > 0 then counter[-1] + 1 else counter[-1]
output accumulator := accumulator[-1] + x
```

### Initialization

Handle the first values properly:

```lola
output safe_previous := if x[-1] != null then x[-1] else 0
output counter := if counter[-1] == null then 1 else counter[-1] + 1
```

## Pattern Examples

### Running Statistics

```lola
input value: Float

output count := if count[-1] == null then 1 else count[-1] + 1
output sum := if sum[-1] == null then value else sum[-1] + value
output mean := sum / count
output variance := ((value - mean) ^ 2 + variance[-1] * (count - 1)) / count
```

### Event Detection

```lola
input temperature: Float

output rising := temperature > temperature[-1]
output falling := temperature < temperature[-1]
output spike := temperature - temperature[-1] > 10.0
```

### State Machines

```lola
input event: String

output state := if event == "start" then "running"
                else if event == "stop" then "stopped"
                else if event == "pause" then "paused"
                else if state[-1] == null then "idle"
                else state[-1]

output valid_transition := 
  (state[-1] == "idle" && state == "running") ||
  (state[-1] == "running" && (state == "paused" || state == "stopped")) ||
  (state[-1] == "paused" && (state == "running" || state == "stopped"))
```

### Threshold Monitoring

```lola
input sensor: Float

output over_threshold := sensor > 100.0
output under_threshold := sensor < 10.0
output in_range := !over_threshold && !under_threshold

output consecutive_violations := 
  if over_threshold then 
    if over_threshold[-1] then consecutive_violations[-1] + 1 else 1
  else 0
```

## Triggers (Future Feature)

Triggers allow you to raise alerts when conditions are met:

```lola
trigger condition "Alert message"

trigger temperature > 100 "Temperature too high!"
trigger battery < 20 "Battery critically low!"
```

## Best Practices

### 1. Use Meaningful Names

```lola
# Good
output battery_level_critical := battery < 20.0

# Bad
output x := y < 20.0
```

### 2. Handle Null Values

Always check for null when using past references:

```lola
output safe_delta := if x[-1] != null then x - x[-1] else 0
```

### 3. Document Complex Logic

Use comments to explain non-obvious specifications:

```lola
# Detect if robot has been stuck for more than 5 time steps
# (velocity near zero but motor commands non-zero)
output stuck := (velocity < 0.1) && (motor_cmd > 0.5) && stuck_counter > 5
```

### 4. Break Down Complex Expressions

Use intermediate streams for clarity:

```lola
# Good - readable
output position_error := target_position - current_position
output error_squared := position_error ^ 2
output rmse := sqrt(error_squared)

# Bad - hard to understand
output rmse := sqrt((target_position - current_position) ^ 2)
```

### 5. Consider Efficiency

Avoid redundant computations:

```lola
# Good - compute once
output sum := x + y
output double_sum := sum * 2

# Bad - compute twice
output double_sum := (x + y) * 2
```

## Common Patterns Library

### Exponential Moving Average

```lola
input value: Float
output ema := if ema[-1] == null 
              then value 
              else 0.9 * ema[-1] + 0.1 * value
```

### Peak Detection

```lola
input signal: Float
output is_peak := signal > signal[-1] && signal > signal[1]
```

### Hysteresis (Schmitt Trigger)

```lola
input value: Float
output high_state := value > 10.0
output low_state := value < 5.0
output state := if high_state then true
                else if low_state then false
                else if state[-1] == null then false
                else state[-1]
```

### Timeout Detection

```lola
input heartbeat: Bool
output timeout := !heartbeat && !heartbeat[-1] && !heartbeat[-2]
```

## Troubleshooting

### Circular Dependencies

**Error**: Circular dependency detected

**Cause**: A stream depends on itself in the same time step

```lola
# Wrong!
output x := y + 1
output y := x + 1
```

**Solution**: Ensure dependencies form a directed acyclic graph (DAG)

### Type Mismatches

**Error**: Type mismatch

**Cause**: Incompatible types in operations

```lola
# Wrong!
output result := int_value + string_value
```

**Solution**: Ensure types are compatible or use type conversions

### Null Pointer Issues

**Problem**: Unexpected null values

**Solution**: Always handle initialization cases:

```lola
output safe := if value[-1] != null then value[-1] else default_value
```

## Next Steps

- See [MQTT Integration](../tutorials/mqtt-integration.md) for practical examples
- Check [Custom Message Types](./custom-messages.md) for working with complex data
- Explore [CLI Options](../reference/cli-options.md) for specification validation
