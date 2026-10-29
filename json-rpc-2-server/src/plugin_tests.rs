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
