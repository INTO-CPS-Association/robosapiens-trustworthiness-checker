The work presented here is supported by the RoboSAPIENS project funded by the European Commission's Horizon Europe programme under grant agreement number 101133807.

## MQTT:
For a minimum example of running with MQTT, run the following specification:
```bash
cargo run -- examples/simple_add.lola --input-mqtt-topics x y --output-mqtt-topics z
```
In MQTT Explorer or similar, send the following message on the topic "x" followed by sending the same message on the topic "y":
```json
{
    "Int": 42
}
```
The following result should be visible on the "z" topic:
```json
{
    "Int": 84
}
```

Note that if you want to provide e.g., an Unknown value then it must be done with:
```json
{
    "Unknown": null
}
```
