use tracing::{debug, info};

#[derive(Clone)]
pub struct ClusterState {
    zk: zookeeper_client::Client,
}

impl ClusterState {
    pub fn new(zk: zookeeper_client::Client) -> ClusterState {
        let cluster = ClusterState { zk };

        let cluster_process = cluster.clone();
        tokio::spawn(async move {
            cluster_process.process().await;
        });

        cluster
    }

    async fn process(&self) {
        let _ = self
            .zk
            .create(
                "/nodes",
                &[],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await;
        let (children, _) = self.zk.get_children("/nodes").await.unwrap();

        let _ = self
            .zk
            .create(
                "/nodes/1",
                &[],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await;

        info!(?children);
    }
}
