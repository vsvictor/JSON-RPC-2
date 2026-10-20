use super::*;
use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, options::*,
    types::FieldTable,
};

pub(super) struct Session {
    pub reply_to: String,
    connection: Connection,
    channel: Channel,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Session {
    fn drop(&mut self) {
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
    ) -> Result<(), TransportError> {
        let mut properties = BasicProperties::default()
            .with_correlation_id(correlation_id.into())
            .with_content_type("application/json".into())
            .with_type(
                if control {
                    "json-rpc-2.ping"
                } else {
                    "json-rpc-2"
                }
                .into(),
            );
        if let Some(reply_to) = reply_to {
            properties = properties.with_reply_to(reply_to.into());
        }
        let confirmation = self
            .channel
            .basic_publish(
                "".into(),
                destination.into(),
                BasicPublishOptions {
                    mandatory: true,
                    immediate: false,
                },
                &payload,
                properties,
            )
            .await
            .map_err(|_| TransportError::new("AMQP publish failed"))?
            .await
            .map_err(|_| TransportError::new("AMQP publisher confirmation failed"))?;
        match confirmation {
            Confirmation::Ack(None) => Ok(()),
            _ => Err(TransportError::new(
                "AMQP publication rejected or unroutable",
            )),
        }
    }
    pub async fn close(&self) -> Result<(), TransportError> {
        self.task.abort();
        if !self.connection.status().connected() {
            return Ok(());
        }
        self.connection
            .close(200, "RPC shutdown".into())
            .await
            .map_err(|_| TransportError::new("AMQP close failed"))
    }
}
pub(super) async fn open(
    config: &AmqpConfig,
    server: bool,
) -> Result<(super::Session, mpsc::Receiver<Message>), TransportError> {
    if config.request_queue.is_empty()
        || config.request_queue.starts_with("amq.")
        || config.prefetch == 0
    {
        return Err(TransportError::new("invalid AMQP RPC queue or prefetch"));
    }
    if !(config.broker_url.starts_with("amqp://") || config.broker_url.starts_with("amqps://")) {
        return Err(TransportError::new("AMQP URL must use amqp:// or amqps://"));
    }
    bounded(config.operation_timeout, async {
        let connection = Connection::connect(&config.broker_url, ConnectionProperties::default())
            .await
            .map_err(|_| TransportError::new("AMQP broker connection failed"))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|_| TransportError::new("AMQP channel creation failed"))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|_| TransportError::new("AMQP confirm setup failed"))?;
        channel
            .basic_qos(config.prefetch, BasicQosOptions::default())
            .await
            .map_err(|_| TransportError::new("AMQP QoS setup failed"))?;
        // Shared request queue remains available while consumers reconnect.
        channel
            .queue_declare(
                config.request_queue.clone().into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|_| TransportError::new("AMQP request queue declaration failed"))?;
        let queue = if server {
            config.request_queue.clone()
        } else {
            channel
                .queue_declare(
                    "".into(),
                    QueueDeclareOptions {
                        exclusive: true,
                        auto_delete: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|_| TransportError::new("AMQP reply queue declaration failed"))?
                .name()
                .to_string()
        };
        let mut consumer = channel
            .basic_consume(
                queue.clone().into(),
                format!("rpc-{}", uuid::Uuid::new_v4()).into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|_| TransportError::new("AMQP consume failed"))?;
        let (incoming, receiver) = mpsc::channel(256);
        let max_payload = config.max_payload_bytes;
        let ack_timeout = config.operation_timeout;
        let task = tokio::spawn(async move {
            while let Some(Ok(delivery)) = consumer.next().await {
                if delivery.data.len() > max_payload
                    || (delivery.properties.reply_to().is_some()
                        && delivery.properties.correlation_id().is_none())
                {
                    let _ = tokio::time::timeout(
                        ack_timeout,
                        delivery.nack(BasicNackOptions {
                            requeue: false,
                            multiple: false,
                        }),
                    )
                    .await;
                    continue;
                }
                let message = Message {
                    payload: delivery.data.into(),
                    reply_to: delivery
                        .properties
                        .reply_to()
                        .as_ref()
                        .map(ToString::to_string),
                    correlation_id: delivery
                        .properties
                        .correlation_id()
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    control: delivery
                        .properties
                        .kind()
                        .as_ref()
                        .is_some_and(|kind| kind.as_str() == "json-rpc-2.ping"),
                    ack: Some(delivery.acker),
                };
                if incoming.send(message).await.is_err() {
                    break;
                }
            }
        });
        Ok((
            super::Session::Amqp(Box::new(Session {
                reply_to: queue,
                connection,
                channel,
                task,
            })),
            receiver,
        ))
    })
    .await
}
