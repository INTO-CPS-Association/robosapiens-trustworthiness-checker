# Installation

This guide will help you install the RoboSAPIENS Trustworthiness Checker and its dependencies.

## Prerequisites

### Required

- **Rust**: Version 1.70 or later
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

- **Git**: To clone the repository
  ```bash
  # Ubuntu/Debian
  sudo apt-get install git
  
  # macOS
  brew install git
  ```

### Optional Dependencies

#### For MQTT Support

- **MQTT Broker**: Such as Mosquitto
  ```bash
  # Ubuntu/Debian
  sudo apt-get install mosquitto mosquitto-clients
  
  # macOS
  brew install mosquitto
  
  # Start the broker
  mosquitto
  ```

- **MQTT Explorer**: For testing and debugging (recommended)
  - Download from [mqtt-explorer.com](http://mqtt-explorer.com/)

#### For ROS2 Support

- **ROS2**: Humble Hawksbill or later
  - Follow the [official ROS2 installation guide](https://docs.ros.org/en/humble/Installation.html)
  - Make sure to source ROS2 in your environment:
    ```bash
    source /opt/ros/humble/setup.bash
    ```

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/robosapiens-trustworthiness-checker.git
cd robosapiens-trustworthiness-checker
```

### 2. Build the Project

For basic functionality (MQTT support):

```bash
cargo build --release
```

For ROS2 support, use the `ros` feature:

```bash
cargo build --release --features ros
```

> **Note**: The first build may take several minutes as Cargo downloads and compiles all dependencies.

### 3. Verify Installation

Run a simple test to verify the installation:

```bash
cargo run -- --help
```

You should see the command-line help output showing available options.

### 4. Run a Basic Example

Test with the simple addition example:

```bash
# Start your MQTT broker in another terminal if not already running
mosquitto

# Run the example
cargo run --release -- examples/simple_add.lola --mqtt-input --mqtt-output
```

If this runs without errors, your installation is successful!

## Building Custom ROS2 Message Types

If you're using ROS2, you'll need to build the custom message types:

```bash
# Make sure ROS2 is sourced
source /opt/ros/humble/setup.bash

# Build custom messages
colcon build

# Source the local workspace
source install/setup.bash
```

## Installing Documentation Tools (Optional)

To build and view this documentation locally:

```bash
# Install mdBook
cargo install mdbook

# Serve the documentation
cd docs
mdbook serve --open
```

The documentation will be available at `http://localhost:3000`.

## Troubleshooting

### Rust Version Issues

If you encounter compilation errors, ensure you have the latest stable Rust:

```bash
rustup update stable
```

### MQTT Connection Issues

If the monitor can't connect to the MQTT broker:

- Verify the broker is running: `ps aux | grep mosquitto`
- Check the default port (1883) is not blocked
- Try connecting manually: `mosquitto_pub -t test -m "hello"`

### ROS2 Build Issues

If colcon build fails:

- Ensure ROS2 is properly sourced
- Check that all ROS2 dependencies are installed
- Try cleaning and rebuilding: `rm -rf build install log && colcon build`

## Next Steps

Now that you have the trustworthiness checker installed, proceed to the [Quick Start](./quick-start.md) guide to learn how to use it!
