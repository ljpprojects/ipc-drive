use std::{io::{self, Read, Write}, thread, time::Duration};

const BLOCK_SIZE: usize = 32 * 1024;

fn main() {
    let mut buffer = [0u8; BLOCK_SIZE];

    let stdout = io::stdout();
    let mut handle = stdout.lock();

    let stdin = io::stdin();
    let mut input = stdin.lock();

    let delay_ms = u64::from_str_radix(&*std::env::args().collect::<Vec<String>>().get(1).unwrap_or(&"500".to_string()), 10).unwrap_or(500);

    loop {
        match input.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                thread::sleep(Duration::from_millis(delay_ms));

                let _ = handle.write_all(&buffer[..n]);
                let _ = handle.flush();
            },
            Err(_) => break,
        }
    }
}
