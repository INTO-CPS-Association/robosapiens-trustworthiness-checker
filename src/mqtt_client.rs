use std::{collections::BTreeMap, time::Duration};

use async_once_cell::OnceCell;
use paho_mqtt as mqtt;
use tokio::sync::{mpsc, oneshot};

type Hostname = String;

/* An interface for creating the MQTT client lazily and sharing a single
 * instance of the client across all whole application (i.e. sharing
 * it between the input provider and the output handler). */

// This background async task listens for requests for MQTT clients and
// provides a unique client for each hostname to requesters
async fn mqtt_client_provider(
    mut client_reqs: mpsc::Receiver<(
        Hostname,
        oneshot::Sender<Result<mqtt::AsyncClient, mqtt::Error>>,
    )>,
) {
    let mut mqtt_clients: BTreeMap<String, paho_mqtt::AsyncClient> = BTreeMap::new();

    while let Some((hostname, tx)) = client_reqs.recv().await {
        // Check if we already have a client for this hostname which we can
        // reuse
        if let Some(client) = mqtt_clients.get(&hostname) {
            if let Err(_) = tx.send(Ok(client.clone())) {
                return;
            }
            continue;
        }

        // Create a new client
        let create_opts = mqtt::CreateOptionsBuilder::new_v3()
            .server_uri(hostname.clone())
            .client_id("robosapiens_trustworthiness_checker")
            .finalize();

        let connect_opts = mqtt::ConnectOptionsBuilder::new_v3()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
            .finalize();

        let mqtt_client = match mqtt::AsyncClient::new(create_opts) {
            Ok(client) => client,
            Err(e) => {
                // (we don't care if the requester has gone away)
                let _e = tx.send(Err(e));
                continue;
            }
        };
        mqtt_clients.insert(hostname.clone(), mqtt_client.clone());

        // Try to connect to the broker
        match mqtt_client.clone().connect(connect_opts).await {
            Ok(_) => {
                // Send the client to the requester
                // (we don't care if the requester has gone away)
                let _e = tx.send(Ok(mqtt_client));
            }
            // If we fail to connect, send the error to the requester
            Err(e) => {
                // (we don't care if the requester has gone away)
                let _e = tx.send(Err(e));
            }
        }
    }
}

static MQTT_CLIENT_REQ_SENDER: OnceCell<
    mpsc::Sender<(
        Hostname,
        oneshot::Sender<Result<mqtt::AsyncClient, mqtt::Error>>,
    )>,
> = OnceCell::new();

pub async fn provide_mqtt_client(hostname: Hostname) -> Result<mqtt::AsyncClient, mqtt::Error> {
    // Create a oneshot channel to receive the client from the background task
    let (tx, rx) = oneshot::channel();

    // Get or initialize the background task and a sender to it
    let req_sender = MQTT_CLIENT_REQ_SENDER.get_or_init(async {
        let (tx, rx) = mpsc::channel(10);
        tokio::spawn(mqtt_client_provider(rx));
        tx
    }).await;

    // Send a request for the client to the background task
    req_sender.send((hostname, tx)).await.unwrap();

    // Wait for the client to be created and return it
    rx.await.unwrap()
}
