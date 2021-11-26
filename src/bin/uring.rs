use std::{
    net::{Ipv6Addr, TcpListener, TcpStream},
    os::unix::prelude::AsRawFd,
    time::{Duration, Instant},
};

use anyhow::Context;
use hdrhistogram::Histogram;
use io_uring::{opcode, types, IoUring};
use structopt::StructOpt;
use tcp_latency::{Args, Ping};
use zerocopy::AsBytes;

const CLIENT_TOKEN: u64 = 0;
const SERVER_TOKEN: u64 = 0;

fn main() -> anyhow::Result<()> {
    let Args {
        single_thread,
        iterations,
    } = Args::from_args();
    anyhow::ensure!(single_thread, "Only single-threaded mode is supported");
    let mut ring = IoUring::new(5).context("IoUring::new")?;

    let listener = TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0)).context("TcpListener::bind")?;
    let address = listener.local_addr().context("TcpListener::local_addr")?;

    // let client = Socket::new(Domain::IPV6, socket2::Type::STREAM, None).context("Socket::new")?;
    let client = TcpStream::connect(address).context("TcpStream::connect")?;

    let (server, _) = listener.accept().context("TcpListener::accept")?;

    let mut histogram = Histogram::<u64>::new(2).context("Get histogram")?;

    let begin = Instant::now();
    println!("Run {} instant ping-pongs", iterations);

    let mut client_buffer = Ping::empty();
    let mut server_buffer = Ping::empty();

    for _ in 0..iterations {
        match single_iteration(
            &mut ring,
            &client,
            &server,
            &mut client_buffer,
            &mut server_buffer,
        )
        .context("single iteration")
        {
            Ok(rtt) => histogram.record(rtt as u64).context("Histogram::record")?,
            Err(e) => {
                let events = ring.submit().context("IoUring::submit")?;
                let mut completion = ring.completion();
                for _ in 0..events {
                    completion
                        .next()
                        .with_context(|| format!("We expected {} completions", events))?;
                }
                return Err(e);
            }
        }
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

    Ok(())
}

fn single_iteration(
    ring: &mut IoUring,
    client: &TcpStream,
    server: &TcpStream,
    client_buffer: &mut Ping,
    server_buffer: &mut Ping,
) -> anyhow::Result<u128> {
    *client_buffer = Ping::new().context("Ping::new")?;
    let send_ping = opcode::Write::new(
        types::Fd(client.as_raw_fd()),
        client_buffer.as_bytes().as_ptr(),
        client_buffer.as_bytes().len() as u32,
    )
    .build()
    .user_data(CLIENT_TOKEN);

    let receive_ping = opcode::Read::new(
        types::Fd(server.as_raw_fd()),
        server_buffer.as_bytes_mut().as_mut_ptr(),
        server_buffer.as_bytes_mut().len() as u32,
    )
    .build()
    .user_data(SERVER_TOKEN);

    let mut submission = ring.submission();
    // Safety: the pointers never become dangling since we wait for submitted
    // entries even in case of an error-caused shirt circuit.
    unsafe {
        submission
            .push(&send_ping)
            .context("SubmissionQueue::push")?;
        submission
            .push(&receive_ping)
            .context("SubmissionQueue::push")?;
    }
    drop(submission);

    ring.submit_and_wait(2)
        .context("IoUring::submit_and_wait")?;
    // Do not care about completion results.

    let send_pong = opcode::Write::new(
        types::Fd(server.as_raw_fd()),
        server_buffer.as_bytes().as_ptr(),
        server_buffer.as_bytes().len() as u32,
    )
    .build()
    .user_data(SERVER_TOKEN);

    let receive_pong = opcode::Read::new(
        types::Fd(client.as_raw_fd()),
        client_buffer.as_bytes_mut().as_mut_ptr(),
        client_buffer.as_bytes_mut().len() as u32,
    )
    .build()
    .user_data(CLIENT_TOKEN);

    let mut submission = ring.submission();
    // Safety: the pointers never become dangling since we wait for submitted
    // entries even in case of an error-caused shirt circuit.
    unsafe {
        submission
            .push(&send_pong)
            .context("SubmissionQueue::push")?;
        submission
            .push(&receive_pong)
            .context("SubmissionQueue::push")?;
    }
    drop(submission);

    ring.submit_and_wait(2)
        .context("IoUring::submit_and_wait")?;
    // Do not care about completion results.

    let elapsed = client_buffer.elapsed().context("Ping::elapsed")?;
    Ok(elapsed)
}
