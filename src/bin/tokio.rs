use std::{
    net::Ipv6Addr,
    time::{Duration, Instant},
};

use anyhow::Context;
use hdrhistogram::Histogram;
use structopt::StructOpt;
use tcp_latency::{Args, Ping};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use zerocopy::AsBytes;

async fn run_server(
    listener: TcpListener,
    terminate: oneshot::Receiver<()>,
) -> std::io::Result<()> {
    tokio::pin!(terminate);
    loop {
        let (mut connection, _) = tokio::select! {
            res = listener.accept() => res?,
            _ = &mut terminate => break,
        };
        let (mut read_half, mut write_half) = connection.split();
        tokio::io::copy(&mut read_half, &mut write_half).await?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let Args {
        single_thread,
        iterations,
    } = Args::from_args();
    let mut builder = if single_thread {
        println!("Running on the current thread");
        tokio::runtime::Builder::new_current_thread()
    } else {
        println!("Running in multiple threads");
        tokio::runtime::Builder::new_multi_thread()
    };
    let rt = builder
        .enable_io()
        .build()
        .context("Building tokio runtime")?;
    {
        let _guard = rt.enter();
        rt.block_on(run(iterations))
    }
}

async fn run(iterations: u32) -> anyhow::Result<()> {
    let server = TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0))
        .await
        .context("Unable to bind the server")?;
    let address = server.local_addr().context("local_addr()")?;

    let (terminate_sender, terminate_receiver) = oneshot::channel();

    let handle = tokio::spawn(run_server(server, terminate_receiver));
    let mut client = TcpStream::connect(address)
        .await
        .context("Can't connect to the server")?;
    let mut histogram = Histogram::<u64>::new(2).context("Get histogram")?;
    let mut recv = Ping::empty();
    let begin = Instant::now();
    println!("Run {} instant ping-pongs", iterations);
    for _ in 0..iterations {
        let now = Ping::new().context("Ping::now()")?;
        client
            .write_all(now.as_bytes())
            .await
            .context("Send ping")?;
        client
            .read_exact(recv.as_bytes_mut())
            .await
            .context("Receive ping")?;
        let rtt = recv.elapsed().context("Get RTT")?;
        histogram.record(rtt as u64).context("Histogram::record")?;
    }
    let elapsed = begin.elapsed();
    println!("Done in {:?}", elapsed);
    println!("RPS: {:.2}", iterations as f64 / elapsed.as_secs_f64());

    let mean = histogram.mean().round() as u64;
    let median = histogram.value_at_quantile(0.5);
    let p95 = histogram.value_at_quantile(0.95);
    let p99 = histogram.value_at_quantile(0.99);
    let max = histogram.max();

    let f = |d| humantime::format_duration(Duration::from_nanos(d));

    println!(
        "\
        mean   = {}\n\
        median = {}\n\
        p95    = {}\n\
        p99    = {}\n\
        max    = {}",
        f(mean),
        f(median),
        f(p95),
        f(p99),
        f(max)
    );

    drop(client);
    let _ = terminate_sender.send(());
    if let Err(e) = handle.await.context("Server task panicked") {
        eprintln!("Server task finished with error: {:#}", e);
    }

    Ok(())
}
