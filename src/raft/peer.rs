use std::net::SocketAddr;

use crate::raft::raft_proto::raft_node_client::RaftNodeClient;

pub struct Peer {
    // index of the next log entry to send to tha/* t */ server
    pub next_index: u64,
    //  index of highest log entry known to be replicated on server
    pub match_index: u64,
    pub addr: SocketAddr,
}

pub async fn connect_to_node(
    address: &SocketAddr,
) -> Result<RaftNodeClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
    let client = RaftNodeClient::connect(address.to_string()).await?;
    Ok(client)
}
