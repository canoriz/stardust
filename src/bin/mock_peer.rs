use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

const FILE_PATH: &str = "fixed_data.bin"; // Configure the file path here
const PIECE_LENGTH: u32 = 16384; // piece length used to compute offsets
const INFO_HASH: [u8; 20] = [0; 20]; // configure if needed
const PEER_ID: &[u8; 20] = b"-MOCKPEER-0000000001"; // 20 bytes

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:6881")?;
    println!(
        "mock_peer listening on 0.0.0.0:6881, serving file: {}",
        FILE_PATH
    );

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                thread::spawn(|| {
                    if let Err(e) = handle_client(s) {
                        eprintln!("connection error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }

    Ok(())
}

fn handle_client(mut stream: TcpStream) -> std::io::Result<()> {
    // Read handshake: pstrlen (1), pstr (pstrlen), reserved (8), info_hash (20), peer_id (20)
    let mut pstrlen_buf = [0u8; 1];
    stream.read_exact(&mut pstrlen_buf)?;
    let pstrlen = pstrlen_buf[0] as usize;

    let mut rest = vec![0u8; pstrlen + 48];
    stream.read_exact(&mut rest)?;

    // Optionally inspect info_hash (located at pstrlen+8 .. pstrlen+28). We ignore.

    // Send handshake back
    let mut handshake = Vec::with_capacity(1 + pstrlen + 48);
    handshake.push(pstrlen as u8);
    handshake.extend_from_slice(&rest[0..pstrlen]); // pstr
    handshake.extend_from_slice(&[0u8; 8]); // reserved
    handshake.extend_from_slice(&INFO_HASH);
    handshake.extend_from_slice(PEER_ID);
    stream.write_all(&handshake)?;

    loop {
        // Read length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf) {
            return Err(e);
        }
        let msg_len = u32::from_be_bytes(len_buf);
        if msg_len == 0 {
            // keep-alive
            continue;
        }

        // Read the message id
        let mut id_buf = [0u8; 1];
        stream.read_exact(&mut id_buf)?;
        let msg_id = id_buf[0];

        let payload_len = (msg_len - 1) as usize;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload)?;
        }

        // Message ID 6 = request
        if msg_id == 6 {
            if payload.len() < 12 {
                eprintln!("malformed request payload (len {})", payload.len());
                continue;
            }
            let index = u32::from_be_bytes(payload[0..4].try_into().unwrap());
            let begin = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let req_len = u32::from_be_bytes(payload[8..12].try_into().unwrap());

            // Compute file offset
            let offset = (index as u64) * (PIECE_LENGTH as u64) + (begin as u64);
            let mut f = match File::open(FILE_PATH) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("failed to open file {}: {}", FILE_PATH, e);
                    continue;
                }
            };
            f.seek(SeekFrom::Start(offset))?;
            let mut block = vec![0u8; req_len as usize];
            // It's okay if fewer bytes are read; read_exact will error — we'll handle that
            if let Err(e) = f.read_exact(&mut block) {
                eprintln!("failed to read block at {} len {}: {}", offset, req_len, e);
                continue;
            }

            // Compose piece message: len prefix, id=7, index(4), begin(4), block
            let mut out = Vec::with_capacity(4 + 1 + 4 + 4 + block.len());
            let total_len = 1u32 + 4 + 4 + (block.len() as u32);
            out.extend_from_slice(&total_len.to_be_bytes());
            out.push(7u8); // piece message id
            out.extend_from_slice(&index.to_be_bytes());
            out.extend_from_slice(&begin.to_be_bytes());
            out.extend_from_slice(&block);

            stream.write_all(&out)?;
        }
        // ignore all other messages
    }
}
