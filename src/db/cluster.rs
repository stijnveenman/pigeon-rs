pub struct ClusterState {
    zk: zookeeper_client::Client,
}

impl ClusterState {
    pub fn new(zk: zookeeper_client::Client) -> ClusterState {
        ClusterState { zk }
    }
}
