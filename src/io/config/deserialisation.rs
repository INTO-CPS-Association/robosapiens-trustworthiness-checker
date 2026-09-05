use std::collections::BTreeMap;

use crate::VarName;

use super::types::{Route, WireRoute};

/// Parse a compact JSON5 route catalog such as `{ "pressure": "/pressure" }` or
/// `{ "pose": ["/pose", "geometry_msgs/msg/Pose"] }`.
pub fn json_to_routes(json: &str) -> anyhow::Result<BTreeMap<VarName, Route>> {
    let routes: BTreeMap<VarName, WireRoute> = json5::from_str(json)
        .map_err(|error| anyhow::anyhow!("route catalog must be a JSON5 object: {error}"))?;
    routes
        .into_iter()
        .map(|(variable, route)| {
            let name = variable.name();
            route
                .into_route()
                .map(|route| (variable, route))
                .map_err(|error| anyhow::anyhow!("invalid route for `{name}`: {error}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_string_and_route_codec_forms() {
        let routes = json_to_routes(
            r#"{
                // JSON is accepted because it is a subset of JSON5.
                pressure: "/robot/sensors/pressure",
                pose: ["/robot/pose", "geometry_msgs/msg/Pose"],
            }"#,
        )
        .unwrap();
        assert_eq!(
            routes[&VarName::new("pressure")].address(),
            "/robot/sensors/pressure"
        );
        assert_eq!(
            routes[&VarName::new("pose")].format().unwrap().as_str(),
            "geometry_msgs/msg/Pose"
        );
    }

    #[test]
    fn rejects_wrong_route_shapes() {
        assert!(json_to_routes(r#"{"x": []}"#).is_err());
        assert!(json_to_routes(r#"{"x": ["/x"]}"#).is_err());
        assert!(json_to_routes(r#"{"x": ["/x", 3]}"#).is_err());
        assert!(json_to_routes(r#"{"x": ""}"#).is_err());
    }
}
