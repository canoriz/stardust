use core::time;
use std::time::Duration;

use stardust::dht::{NodeID, Resp, RpcAddr, DHT};
use tokio::{
    io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader},
    time::timeout,
};
use tracing::info;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .init();
    info!("ww");

    let id = [
        0xdf, 0x79, 0x5e, 0x52, 0xbe, 0x15, 0xd3, 0x86, 0x62, 0x37, 0x6c, 0xaa, 0xd3, 0x07, 0xa8,
        0x9d, 0xaf, 0x58, 0x0e, 0xbd,
    ];

    // let target = [
    //     0x67, 0x66, 0x12, 0x53, 0x4e, 0x48, 0xd1, 0xca, 0x79, 0x29, 0x50, 0xb0, 0xd6, 0x6f, 0xd3,
    //     0xbb, 0x73, 0xfa, 0x11, 0x7e,
    // ];

    let client = DHT::new(id, 49999, "ST01".into());
    let r = timeout(
        time::Duration::from_millis(5000),
        client.ping_rpc(
            RpcAddr::NoID(
                "[240e:b8f:5c68:8400:4c07:3e69:7b5a:741]:59999"
                    .parse()
                    .unwrap(),
            ),
            time::Duration::from_secs(4),
        ),
    )
    .await
    .unwrap();
    println!("ping {:?}", r);

    let r = client
        .find_node_rpc(
            RpcAddr::NoID(
                "[240e:b8f:5c68:8400:4c07:3e69:7b5a:741]:59999"
                    .parse()
                    .unwrap(),
            ),
            id,
            time::Duration::from_secs(4),
        )
        .await
        .unwrap();
    println!("find node {:?}", r);

    // Wrap it in a BufReader for efficient line-by-line reading.
    let mut reader = BufReader::new(stdin()).lines();
    println!("--- Tokio Async Line Processor ---");
    println!("Enter lines of text. Press Ctrl+D (or Ctrl+Z on Windows) to send EOF and exit.");
    print!("> ");
    // Ensure the initial prompt is visible immediately
    stdout().flush().await?;

    // Asynchronously iterate over the lines of input.
    while let Some(line_result) = reader.next_line().await? {
        // The line has been successfully read (it returns a Result).
        let line = line_result;

        // Process the line asynchronously.
        let processed_line = process_line_async(line, &client).await;

        println!("\nProcessed: {}", processed_line);
        print!("> ");
        // Flush stdout to make sure the prompt appears right away,
        // which is crucial for interactive console apps.
        stdout().flush().await?;
    }

    // When the loop exits, it means `reader.next_line().await?` returned None,
    // indicating that EOF (End-of-File) was received.
    println!("\n--- EOF received. Shutting down. ---");
    Ok(())
}

async fn process_line_async(line: String, client: &DHT) -> String {
    let (cmd, addr, id) = {
        let v: Vec<_> = line.split_ascii_whitespace().collect();
        if v.len() != 3 {
            return format!("input cmd ip id, get `{}`, {v:?}", line);
        }
        (v[0], v[1], v[2])
    };

    let id = if let Some(id) = str2nid(id) {
        id
    } else {
        return "NodeID should in hex format".into();
    };
    let addr = if let Ok(a) = addr.parse() {
        a
    } else {
        return "addr format wrong".into();
    };

    let res = if cmd.starts_with("p") {
        client
            .ping_rpc(RpcAddr::no_id(addr), Duration::from_secs(5))
            .await
    } else if cmd.starts_with("f") {
        client
            .find_node_rpc(RpcAddr::no_id(addr), id, Duration::from_secs(5))
            .await
    } else if cmd.starts_with("g") {
        client
            .get_peers_rpc(RpcAddr::no_id(addr), id, Duration::from_secs(5))
            .await
    } else {
        return "command should be p, f or g".into();
    };

    match res {
        Ok(r) => format!("result: {:?}", r),
        Err(e) => format!("error {:?}", e),
    }
}

fn str2nid(id: &str) -> Option<NodeID> {
    let mut nid = [0u8; 20];
    match hex::decode_to_slice(id, &mut nid) {
        Ok(_) => Some(nid),
        Err(_) => None,
    }
}
