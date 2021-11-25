use std::{
    io::{ErrorKind, Read, Write},
    net::{Ipv6Addr, TcpListener, TcpStream},
    time::{Duration, Instant},
};

use anyhow::Context;
use hdrhistogram::Histogram;
use structopt::StructOpt;
use tcp_latency::{Args, Ping};
use zerocopy::AsBytes;

fn main() -> anyhow::Result<()> {
    let Args {
        single_thread,
        iterations,
    } = Args::from_args();

    let listener = TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0)).context("TcpListener::bind")?;
    let address = listener.local_addr().context("TcpListener::local_addr")?;
    let client = TcpStream::connect(address).context("TcpStream::connect")?;

    let histogram = Histogram::new(2).context("Histogram::new")?;

    let f = if single_thread {
        run_single_thread
    } else {
        run_multi_thread
    };

    let begin = Instant::now();

    let histogram = f(listener, client, histogram, iterations)?;

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

    Ok(())
}

fn run_single_thread(
    listener: TcpListener,
    mut client: TcpStream,
    mut histogram: Histogram<u64>,
    iterations: u32,
) -> anyhow::Result<Histogram<u64>> {
    let (mut server, _) = listener.accept().context("TcpListener::accept")?;

    for _ in 0..iterations {
        let mut ping = Ping::new().context("Ping::new")?;
        client
            .write_all(ping.as_bytes())
            .context("client.write_all")?;

        server
            .read_exact(ping.as_bytes_mut())
            .context("server.read_exact")?;
        server
            .write_all(ping.as_bytes())
            .context("server.write_all")?;

        client
            .read_exact(ping.as_bytes_mut())
            .context("client.read_exact")?;
        let elapsed = ping.elapsed().context("Ping::elapsed")?;
        histogram
            .record(elapsed as u64)
            .context("Histogram::record")?;
    }

    Ok(histogram)
}

fn run_multi_thread(
    listener: TcpListener,
    client: TcpStream,
    histogram: Histogram<u64>,
    iterations: u32,
) -> anyhow::Result<Histogram<u64>> {
    let client_handle = std::thread::spawn(move || client_thread(client, iterations, histogram));
    let server_handle = std::thread::spawn(|| server_thread(listener));

    let histogram = client_handle.join().expect("Client thread panicked")?;

    if let Err(e) = server_handle.join().expect("Server thread panicked") {
        eprintln!("Server thread terminated with error: {:#}", e);
    }
    Ok(histogram)
}

fn client_thread(
    mut client: TcpStream,
    iterations: u32,
    mut histogram: Histogram<u64>,
) -> anyhow::Result<Histogram<u64>> {
    for _ in 0..iterations {
        let mut ping = Ping::new().context("Ping::new")?;
        client
            .write_all(ping.as_bytes())
            .context("I/O error while writing data")?;
        client
            .read_exact(ping.as_bytes_mut())
            .context("I/O error while reading data")?;
        let elapsed = ping.elapsed().context("Calculating elapsed")?;
        histogram
            .record(elapsed as u64)
            .context("Histogram::record")?;
    }
    Ok(histogram)
}

fn server_thread(listener: TcpListener) -> anyhow::Result<()> {
    let (mut connection, _) = listener.accept().context("TcpListener::accept")?;

    let mut ping_buffer = Ping::empty();
    loop {
        match connection.read_exact(ping_buffer.as_bytes_mut()) {
            Ok(()) => { /* Fallthrough */ }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => anyhow::bail!("I/O error while reading data: {:#}", e),
        };
        connection
            .write_all(ping_buffer.as_bytes())
            .context("I/O error while writing data")?;
    }

    Ok(())
}
