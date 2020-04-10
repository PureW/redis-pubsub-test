use futures::StreamExt;
use redis;
use redis::Commands;

const REDIS_ENDPOINT: &'static str = "redis://localhost:6379/";
const PAYLOAD: &'static str = "bar bar bar bar";

fn main() {
    run_publisher();

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
            res.unwrap();
            if num_published % 100_000 == 0 {
                println!("PUBL: Published {} msgs", num_published);
            }
        }
    });
    // By removing this sleep, the program runs as expected since the
    // consumer comes online first
    std::thread::sleep(std::time::Duration::from_millis(200));
}

#[tokio::main]
async fn run_subscriber() {
    let client = redis::Client::open(REDIS_ENDPOINT).unwrap();
    let mut pubsub = client.get_async_connection().await.unwrap().into_pubsub();
    pubsub.subscribe("foo").await.unwrap();

    let mut pubsub_stream = pubsub.on_message();
    let mut num_received:u64 = 0;
    println!("SUBS: Listenin for msgs");

    loop {
        print!(".");
        let redis_msg = match pubsub_stream.next().await {
            Some(redis_msg) => redis_msg,

            None => {
                println!("SUBS: No pubsub message available, go back to waiting");
                continue;
            }
        };
        // Subscribe to channel, but consume slower than publish
        let msg_body: u64 = redis_msg.get_payload().unwrap();
        num_received += 1;
        if num_received % 10 == 0 {
            println!("SUBS: Received {} msgs. Latest msg: {}", num_received, msg_body);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
