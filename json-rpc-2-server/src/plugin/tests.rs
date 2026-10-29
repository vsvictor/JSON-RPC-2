use super::*;

struct Hello;

impl ServerPlugin for Hello {
    const NAME: &'static str = "hello";

    async fn install(self, server: &RpcServer) -> anyhow::Result<PluginHandle> {
        server.register("hello", |_| async { Ok(serde_json::json!("world")) })?;
        Ok(PluginHandle::new().with_maintenance(Duration::from_secs(1), (), |()| async { Ok(()) }))
    }
}

#[tokio::test]
async fn plugin_registers_methods_on_the_shared_server() {
    let server = RpcServer::new();
    let plugin = BoxedPlugin::new(Hello);
    assert_eq!(plugin.name(), "hello");
    let handle = plugin.install(&server).await.unwrap();
    assert!(server.method_names().contains(&"hello".to_owned()));
    assert_eq!(handle.maintenance().len(), 1);
    assert_eq!(handle.maintenance()[0].interval(), Duration::from_secs(1));
    handle.maintenance()[0].run().await.unwrap();
}

#[tokio::test]
async fn register_with_clones_state_and_aliases_share_handlers() {
    let server = RpcServer::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    server
        .register_with("count", calls.clone(), |calls, _| async move {
            Ok((calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1).into())
        })
        .unwrap();
    server.register_aliases("rpc.", &["count"]).unwrap();
    assert!(server.register_aliases("rpc.", &["missing"]).is_err());
    for method in ["count", "rpc.count"] {
        let request = serde_json::json!({"jsonrpc": "2.0", "method": method, "id": 1});
        let response = server
            .handle_bytes(request.to_string().as_bytes())
            .await
            .unwrap();
        assert!(!response.is_empty());
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}
