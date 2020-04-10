use redis::Commands;

const PAYLOAD: &'static str = "bar bar bar bar";

fn main() {
    let redis_client = redis::Client::open("redis://localhost:6379/").unwrap();
    let mut redis_conn = redis_client.get_connection().unwrap();

    let pub_client = redis_client.clone();
    std::thread::spawn(move || {
        let mut conn = pub_client.get_connection().unwrap();
        // Set up a publisher that fills up subscriber buffers
        loop {
            let res: Result<(), _> = conn.publish("foo", PAYLOAD);
            res.unwrap();
        }
    });

    let mut pubsub = redis_conn.as_pubsub();
    pubsub.subscribe("foo").unwrap();
    loop {
        // Subscribe to channel, but consume slower than publish
        match pubsub.get_message() {
            Ok(msg) => {
                let msg_body: String = msg.get_payload().unwrap();
                if msg_body != PAYLOAD {
                    println!("Payload differs: {}", msg_body);
                }
            }
            Err(err) => {
                println!("failed reading pubsub message: {}", err);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
