use super::*;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Outgoing, QoS, SubscribeReasonCode};
use std::collections::VecDeque;
use tokio::sync::oneshot;

struct Publish {
    destination: String,
    data: Vec<u8>,
    done: oneshot::Sender<Result<(), json_rpc_2_transport::TransportError>>,
}

pub(super) struct Session {
    pub reply_to: String,
    commands: mpsc::Sender<Publish>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}

impl Session {
    pub async fn publish(
        &self,
        destination: &str,
        payload: Bytes,
        reply_to: Option<String>,
        correlation_id: String,
        control: bool,
        metadata: Metadata,
    ) -> Result<(), json_rpc_2_transport::TransportError> {
        let data = serde_json::to_vec(&Envelope {
            payload: payload.to_vec(),
            reply_to,
            correlation_id,
            control,
            metadata,
        })
        .map_err(|_| json_rpc_2_transport::TransportError::new("MQTT envelope encoding failed"))?;
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Publish {
                destination: destination.into(),
                data,
                done,
            })
            .await
            .map_err(|_| json_rpc_2_transport::closed())?;
        wait.await.map_err(|_| json_rpc_2_transport::closed())?
    }

    pub async fn close(&self) -> Result<(), json_rpc_2_transport::TransportError> {
        self.stop.cancel();
        while !self.task.is_finished() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

pub(super) async fn open(
    config: &MqttConfig,
) -> Result<(super::Session, mpsc::Receiver<Message>), json_rpc_2_transport::TransportError> {
    if config.request_topic.is_empty() || config.request_topic.contains(['#', '+', '\0']) {
        return Err(json_rpc_2_transport::TransportError::new(
            "MQTT RPC request topic must be a nonempty exact topic",
        ));
    }
    if !(config.broker_url.starts_with("mqtt://") || config.broker_url.starts_with("mqtts://")) {
        return Err(json_rpc_2_transport::TransportError::new(
            "MQTT URL must use mqtt:// or mqtts://",
        ));
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let separator = if config.broker_url.contains('?') {
        '&'
    } else {
        '?'
    };
    let url = format!("{}{separator}client_id=rpc-{id}", config.broker_url);
    let mut options = MqttOptions::parse_url(url)
        .map_err(|_| json_rpc_2_transport::TransportError::new("invalid MQTT broker URL"))?;
    options.set_clean_session(true);
    options.set_keep_alive(Duration::from_secs(15));
    let packet_limit = config
        .max_payload_bytes
        .saturating_mul(4)
        .saturating_add(8192);
    options.set_max_packet_size(packet_limit, packet_limit);
    if let Some(ca) = &config.ca_certificate {
        if !config.broker_url.starts_with("mqtts://") {
            return Err(json_rpc_2_transport::TransportError::new(
                "MQTT CA requires mqtts://",
            ));
        }
        options.set_transport(rumqttc::Transport::tls(ca.clone(), None, None));
    }
    let (client, mut eventloop) = AsyncClient::new(options, 256);
    let reply_to = format!("json-rpc-2/replies/{id}");
    let (incoming, receiver) = mpsc::channel(256);
    let (commands, mut command_rx) = mpsc::channel::<Publish>(64);
    let (ready, ready_rx) = oneshot::channel();
    let mut ready = Some(ready);
    let stop = CancellationToken::new();
    let cancel = stop.clone();
    let subscription = reply_to.clone();
    let max_payload = config.max_payload_bytes;
    let task = tokio::spawn(async move {
        let mut waiting = VecDeque::new();
        let mut published = HashMap::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = client.disconnect().await;
                    let _ = tokio::time::timeout(Duration::from_secs(1), async {
                        while let Ok(event) = eventloop.poll().await {
                            if matches!(event, Event::Outgoing(Outgoing::Disconnect)) { break; }
                        }
                    }).await;
                    break;
                },
                command = command_rx.recv() => match command {
                    Some(command) => {
                        if client.publish(command.destination, QoS::AtLeastOnce, false, command.data).await.is_err() { break; }
                        waiting.push_back(command.done);
                    },
                    None => break,
                },
                event = eventloop.poll() => match event {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        if client.subscribe(&subscription, QoS::AtLeastOnce).await.is_err() { break; }
                    },
                    Ok(Event::Incoming(Incoming::SubAck(ack))) => {
                        if ack.return_codes.iter().any(|code| matches!(code, SubscribeReasonCode::Failure)) { break; }
                        if let Some(ready) = ready.take() { let _ = ready.send(()); }
                    },
                    Ok(Event::Outgoing(Outgoing::Publish(id))) => {
                        if let std::collections::hash_map::Entry::Vacant(entry) = published.entry(id)
                            && let Some(done) = waiting.pop_front() {
                            entry.insert(done);
                        }
                    },
                    Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                        if let Some(done) = published.remove(&ack.pkid) { let _ = done.send(Ok(())); }
                    },
                    Ok(Event::Incoming(Incoming::Publish(packet))) => {
                        if packet.retain { continue; }
                        if let Ok(envelope) = serde_json::from_slice::<Envelope>(&packet.payload) {
                            if envelope.payload.len() > max_payload || envelope.correlation_id.len() > 256 { continue; }
                            if envelope.reply_to.as_ref().is_some_and(|topic| !topic.starts_with("json-rpc-2/replies/") || topic.contains(['#', '+', '\0'])) { continue; }
                            if envelope.metadata.session.as_ref().is_some_and(|session| !session.push_to.starts_with("json-rpc-2/replies/") || session.push_to.contains(['#', '+', '\0'])) { continue; }
                            let message = Message {
                                payload: envelope.payload.into(),
                                correlation_id: envelope.correlation_id,
                                metadata: envelope.metadata,
                                #[cfg(feature = "amqp")]
                                ack: None,
                            };
                            if incoming.try_send(message).is_err() { break; }
                        }
                    },
                    Ok(_) => {},
                    Err(_) => break,
                },
            }
        }
    });
    let session = Session {
        reply_to,
        commands,
        stop,
        task,
    };
    super::bounded(config.operation_timeout, async {
        ready_rx.await.map_err(|_| {
            json_rpc_2_transport::TransportError::new("MQTT connect or subscribe failed")
        })
    })
    .await?;
    Ok((super::Session::Mqtt(session), receiver))
}
