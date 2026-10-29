//! Contract for optional method sets that a host installs into an [`RpcServer`].
//!
//! A plugin owns its configuration and registers its methods during
//! [`ServerPlugin::install`]; the host only decides which plugins to add. Plugins depend on
//! this crate alone, never on each other or on the host.
//!
//! ```
//! use json_rpc_2_server::{PluginHandle, RpcServer, ServerPlugin};
//!
//! struct Hello;
//!
//! impl ServerPlugin for Hello {
//!     const NAME: &'static str = "hello";
//!
//!     async fn install(self, server: &RpcServer) -> anyhow::Result<PluginHandle> {
//!         server.register("hello", |_| async { Ok("world".into()) })?;
//!         Ok(PluginHandle::new())
//!     }
//! }
//! ```

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use crate::RpcServer;

pub type PluginFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

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

    /// Adds a periodic task; `task` receives a clone of `state` on every run.
    pub fn with_maintenance<S, F, Fut>(mut self, interval: Duration, state: S, task: F) -> Self
    where
        S: Clone + Send + Sync + 'static,
        F: Fn(S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.maintenance.push(MaintenanceTask {
            interval,
            run: Arc::new(move || Box::pin(task(state.clone()))),
        });
        self
    }

    pub fn maintenance(&self) -> &[MaintenanceTask] {
        &self.maintenance
    }
}

/// An optional set of methods installed into a shared [`RpcServer`] at startup.
///
/// `RpcServer` clones share one registry, so a plugin may keep the clone it is given.
/// A method-name collision is an error; discard the server because earlier registrations may
/// already have succeeded.
pub trait ServerPlugin: Send + 'static {
    const NAME: &'static str;

    fn install(
        self,
        server: &RpcServer,
    ) -> impl Future<Output = anyhow::Result<PluginHandle>> + Send;
}

type InstallFn = Box<dyn for<'a> FnOnce(&'a RpcServer) -> PluginFuture<'a, PluginHandle> + Send>;

/// Type-erased [`ServerPlugin`] for hosts that keep a list of different plugins.
pub struct BoxedPlugin {
    name: &'static str,
    install: InstallFn,
}

impl BoxedPlugin {
    pub fn new<P: ServerPlugin>(plugin: P) -> Self {
        Self {
            name: P::NAME,
            install: Box::new(move |server| Box::pin(plugin.install(server))),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn install(self, server: &RpcServer) -> PluginFuture<'_, PluginHandle> {
        (self.install)(server)
    }
}

#[cfg(test)]
mod tests;
