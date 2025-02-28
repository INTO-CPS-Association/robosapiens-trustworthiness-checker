use std::time::Duration;

use async_stream::stream;
use futures::{StreamExt, stream::BoxStream};
use paho_mqtt::{self as mqtt, Message};
use tracing::{Level, debug, info, instrument, warn};
use uuid::Uuid;

type Hostname = String;

/* An interface for creating the MQTT client lazily and sharing a single
 * instance of the client across all whole application (i.e. sharing
 * it between the input provider and the output handler). */

#[instrument(level=Level::INFO, skip(client))]
fn message_stream(mut client: mqtt::AsyncClient) -> BoxStream<'static, Message> {
    Box::pin(stream! {
        loop {
            let mut stream = client.get_stream(10);
            loop {
                match stream.next().await {
                    Some(msg) => {
                        let msg = msg.expect("Expecting a correct message");
                        debug!(name: "Received MQTT message", ?msg, topic = msg.topic());
                        yield msg;
                    }
                    None => {
                        break;
                    }
                }
            }
            warn!("Connection list. Attempting reconnect...");
            while let Err(err) = client.reconnect().await {
                warn!(name: "MQTT client reconnection failed", ?err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            info!("MQTT client reconnected");
        }
    })
}

pub async fn provide_mqtt_client_with_subscription(
    hostname: Hostname,
) -> Result<(mqtt::AsyncClient, BoxStream<'static, Message>), mqtt::Error> {
    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(hostname.clone())
        .client_id(format!(
            "robosapiens_trustworthiness_checker_{}",
            Uuid::new_v4()
        ))
        .finalize();

    let connect_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .keep_alive_interval(Duration::from_secs(30))
        .clean_session(false)
        .finalize();

    let mqtt_client = match mqtt::AsyncClient::new(create_opts) {
        Ok(client) => client,
        Err(e) => {
            return Err(e);
        }
    };

    debug!(
        name = "Created MQTT client",
        ?hostname,
        client_id = mqtt_client.client_id()
    );

    let stream = message_stream(mqtt_client.clone());
    debug!(
        name = "Started consuming MQTT messages",
        ?hostname,
        client_id = mqtt_client.client_id()
    );

    // Try to connect to the broker
    mqtt_client
        .clone()
        .connect(connect_opts)
        .await
        .map(|_| (mqtt_client, stream))
}

pub async fn provide_mqtt_client(hostname: Hostname) -> Result<mqtt::AsyncClient, mqtt::Error> {
    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(hostname.clone())
        .client_id(format!(
            "robosapiens_trustworthiness_checker_{}",
            Uuid::new_v4()
        ))
        .finalize();

    let connect_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .keep_alive_interval(Duration::from_secs(30))
        .clean_session(false)
        .finalize();

    let mqtt_client = match mqtt::AsyncClient::new(create_opts) {
        Ok(client) => client,
        Err(e) => {
            // (we don't care if the requester has gone away)
            return Err(e);
        }
    };

    debug!(
        name = "Created MQTT client",
        ?hostname,
        client_id = mqtt_client.client_id()
    );

    // Try to connect to the broker
    mqtt_client
        .clone()
        .connect(connect_opts)
        .await
        .map(|_| mqtt_client)
}
