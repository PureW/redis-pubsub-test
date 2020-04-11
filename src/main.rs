use redis::Commands;

const REDIS_ENDPOINT: &'static str = "redis://localhost:6379";
const PAYLOAD: &'static str = "bar bar bar bar";

fn main() {
    for _ in 0..5 {
        run_publisher();
    }

    // By removing this sleep, the program runs as expected since the
    // consumer comes online first
    //std::thread::sleep(std::time::Duration::from_millis(200));
    run_subscriber();
}

fn run_publisher() {
    let client = redis::Client::open(REDIS_ENDPOINT).unwrap();
    std::thread::spawn(move || {
        let mut conn = client.get_connection().unwrap();
        // Set up a publisher that fills up subscriber buffers
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut num_published = 0;
        loop {
            num_published += 1;
            let res: Result<(), _> = conn.publish("foo", num_published);
            if let Err(err) = res {
                println!("ERROR: Publish failed {}", err);
                match client.get_connection() {
                    Ok(new_conn) => {
                        conn = new_conn;
                        println!("PUBL: Aquired a new connection");
                    }
                    Err(err) => {
                        println!("ERROR: Could not acquire new conn: {}", err);
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
                continue;
            }
            if num_published % 100_000 == 0 {
                println!(
                    "PUBL: Published {} msgs ({}Mb)",
                    num_published,
                    num_published / std::mem::size_of::<u64>() / 1024 / 1024,
                );
            }
        }
    });
}

fn run_subscriber() {
    let client = redis::Client::open(REDIS_ENDPOINT).unwrap();
    let mut conn = client.get_connection().unwrap();
    let mut pubsub = conn.as_pubsub();
    pubsub.subscribe("foo").unwrap();
    loop {
        // Subscribe to channel, but consume slower than publish
        match pubsub.get_message() {
            Ok(msg) => {
                let _: u64 = msg.get_payload().unwrap();
            }
            Err(err) => {
                println!("failed reading pubsub message: {}", err);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
