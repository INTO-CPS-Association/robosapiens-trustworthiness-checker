use std::time::Duration;

use async_stream::stream;
use futures::{stream::BoxStream, StreamExt};
use paho_mqtt::{self as mqtt, Message};

type Hostname = String;

/* An interface for creating the MQTT client lazily and sharing a single
 * instance of the client across all whole application (i.e. sharing
 * it between the input provider and the output handler). */

fn message_stream(mut client: mqtt::AsyncClient) -> BoxStream<'static, Message> {
    Box::pin(stream! {
        loop {
            let mut stream = client.get_stream(10);
            loop {
                match stream.next().await {
                    Some(msg) => {
                        let msg = msg.expect("Expecting a correct message");
                        println!(
                            "[MQTT Stream] Received message: {:?} on {:?}",
                            msg,
                            msg.topic()
                        );
                        yield msg;
                    }
                    None => {
                        break;
                    }
                }
            }
            println!("[MQTT Client Provider] Connection lost. Attempting reconnect...");
            while let Err(e) = client.reconnect().await {
                println!("[MQTT Client Provider] Reconnection attempt failed: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            println!("[MQTT Client Provider] Reconnected");
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
            rand::random::<u16>()
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

    println!(
        "[MQTT Client Provider] Created client for hostname: {} with client_id: {}",
        hostname,
        mqtt_client.client_id()
    );

    let stream = message_stream(mqtt_client.clone());
    println!(
        "[MQTT Client Provider] Started consuming for hostname: {}",
        hostname
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
            rand::random::<u16>()
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

    println!(
        "[MQTT Client Provider] Created client for hostname: {} with client_id: {}",
        hostname,
        mqtt_client.client_id()
    );

    // Try to connect to the broker
    mqtt_client
        .clone()
        .connect(connect_opts)
        .await
        .map(|_| mqtt_client)
}
