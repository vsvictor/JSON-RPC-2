//! Contract for optional method sets that a host installs into an [`RpcServer`].
//!
//! A plugin owns its configuration and registers its methods during
//! [`ServerPlugin::install`]; the host only decides which plugins to add. Plugins depend on
//! this crate alone, never on each other or on the host.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use crate::RpcServer;

pub type PluginError = Box<dyn std::error::Error + Send + Sync>;
pub type PluginFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, PluginError>> + Send + 'a>>;

type MaintenanceFn = dyn Fn() -> PluginFuture<'static, ()> + Send + Sync;

/// A periodic task a plugin asks its host to run, for example expired-session cleanup.
#[derive(Clone)]
pub struct MaintenanceTask {
    interval: Duration,
    run: Arc<MaintenanceFn>,
}

impl MaintenanceTask {
    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn run(&self) -> PluginFuture<'static, ()> {
        (self.run)()
    }
}

/// What a plugin leaves with the host after installation.
#[derive(Clone, Default)]
pub struct PluginHandle {
    maintenance: Vec<MaintenanceTask>,
}

impl PluginHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_maintenance<F>(mut self, interval: Duration, task: F) -> Self
    where
        F: Fn() -> PluginFuture<'static, ()> + Send + Sync + 'static,
    {
        self.maintenance.push(MaintenanceTask {
            interval,
            run: Arc::new(task),
        });
        self
    }

    pub fn maintenance(&self) -> &[MaintenanceTask] {
        &self.maintenance
    }
}

/// An optional set of methods installed into a shared [`RpcServer`] at startup.
///
/// `RpcServer` clones share one registry, so a plugin may keep or wrap the clone it is given.
/// A method-name collision is an error; discard the server because earlier registrations may
/// already have succeeded.
pub trait ServerPlugin: Send + 'static {
    fn name(&self) -> &'static str;

    fn install<'a>(self: Box<Self>, server: &'a RpcServer) -> PluginFuture<'a, PluginHandle>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Hello;

    impl ServerPlugin for Hello {
        fn name(&self) -> &'static str {
            "hello"
        }

        fn install<'a>(self: Box<Self>, server: &'a RpcServer) -> PluginFuture<'a, PluginHandle> {
            Box::pin(async move {
                server.register("hello", |_| async { Ok(serde_json::json!("world")) })?;
                Ok(PluginHandle::new()
                    .with_maintenance(Duration::from_secs(1), || Box::pin(async { Ok(()) })))
            })
        }
    }

    #[tokio::test]
    async fn plugin_registers_methods_on_the_shared_server() {
        let server = RpcServer::new();
        let plugin: Box<dyn ServerPlugin> = Box::new(Hello);
        assert_eq!(plugin.name(), "hello");
        let handle = plugin.install(&server).await.unwrap();
        assert!(server.method_names().contains(&"hello".to_owned()));
        assert_eq!(handle.maintenance().len(), 1);
        assert_eq!(handle.maintenance()[0].interval(), Duration::from_secs(1));
        handle.maintenance()[0].run().await.unwrap();
    }
}
