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

fn wait_for_submissions(ring: &mut IoUring) -> anyhow::Result<()> {
    let events = ring.submit().context("IoUring::submit")?;
    let mut completion = ring.completion();
    for _ in 0..events {
        completion
            .next()
            .with_context(|| format!("We expected {} completions", events))?;
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let Args {
        single_thread,
        iterations,
    } = Args::from_args();

    let listener = TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0)).context("TcpListener::bind")?;
    let address = listener.local_addr().context("TcpListener::local_addr")?;

    // let client = Socket::new(Domain::IPV6, socket2::Type::STREAM, None).context("Socket::new")?;
    let client = TcpStream::connect(address).context("TcpStream::connect")?;

    let (server, _) = listener.accept().context("TcpListener::accept")?;

    let histogram = Histogram::<u64>::new(2).context("Get histogram")?;

    let f = if single_thread {
        run_single_thread
    } else {
        run_multi_thread
    };

    let begin = Instant::now();
    println!("Run {} instant ping-pongs", iterations);

    let histogram = f(client, server, iterations, histogram).context("run")?;

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
    client: TcpStream,
    server: TcpStream,
    iterations: u32,
    mut histogram: Histogram<u64>,
) -> anyhow::Result<Histogram<u64>> {
    let mut ring = IoUring::new(5).context("IoUring::new")?;

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
                wait_for_submissions(&mut ring)?;
                return Err(e);
            }
        }
    }

    Ok(histogram)
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
    ring.completion().for_each(drop);

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
    ring.completion().for_each(drop);

    let elapsed = client_buffer.elapsed().context("Ping::elapsed")?;
    Ok(elapsed)
}

fn run_multi_thread(
    client: TcpStream,
    server: TcpStream,
    iterations: u32,
    histogram: Histogram<u64>,
) -> anyhow::Result<Histogram<u64>> {
    let client_handle = std::thread::spawn(move || client_thread(client, histogram, iterations));
    let serer_handle = std::thread::spawn(|| server_thread(server));

    let client_result = client_handle.join().expect("Client thread panicked");
    if let Err(e) = serer_handle.join().expect("Server thread panicked") {
        eprintln!("Server thread terminated with an error: {:#}", e)
    }
    client_result
}

fn client_thread(
    client: TcpStream,
    mut histogram: Histogram<u64>,
    iterations: u32,
) -> anyhow::Result<Histogram<u64>> {
    let mut ring = IoUring::new(2).context("IoUring::new")?;
    let mut ping_buffer = Ping::empty();
    let mut pong_buffer = Ping::empty();

    for _ in 0..iterations {
        match client_thread_once(&mut ring, &client, &mut ping_buffer, &mut pong_buffer)
            .context("client_thread_once")
        {
            Ok(rtt) => histogram.record(rtt as u64).context("Histogram::record")?,
            Err(e) => {
                wait_for_submissions(&mut ring)?;
                return Err(e);
            }
        }
    }

    Ok(histogram)
}

fn client_thread_once(
    ring: &mut IoUring,
    client: &TcpStream,
    ping_buffer: &mut Ping,
    pong_buffer: &mut Ping,
) -> anyhow::Result<u128> {
    *ping_buffer = Ping::new().context("Ping::new")?;
    let send_ping = opcode::Write::new(
        types::Fd(client.as_raw_fd()),
        ping_buffer.as_bytes().as_ptr(),
        ping_buffer.as_bytes().len() as u32,
    )
    .build();

    let receive_pong = opcode::Read::new(
        types::Fd(client.as_raw_fd()),
        pong_buffer.as_bytes_mut().as_mut_ptr(),
        pong_buffer.as_bytes_mut().len() as u32,
    )
    .build();

    let mut submission = ring.submission();
    // Safety: the pointers never become dangling since we wait for submitted
    // entries even in case of an error-caused shirt circuit.
    unsafe {
        submission
            .push(&send_ping)
            .context("SubmissionQueue::push")?;
        submission
            .push(&receive_pong)
            .context("SubmissionQueue::push")?;
    };
    drop(submission);
    ring.submit_and_wait(2)
        .context("IoUring::submit_and_wait")?;
    // Do not care about completion results.
    ring.completion().for_each(drop);

    pong_buffer.elapsed().context("Ping::elapsed")
}

fn server_thread(server: TcpStream) -> anyhow::Result<()> {
    let mut ring = IoUring::new(1).context("IoUring::new")?;
    let mut ping_buffer = Ping::empty();

    loop {
        match server_thread_once(&mut ring, &server, &mut ping_buffer).context("server_thread_once")
        {
            Ok(true) => { /* Continue */ }
            Ok(false) => break,
            Err(e) => {
                wait_for_submissions(&mut ring)?;
                return Err(e);
            }
        }
    }

    Ok(())
}

fn server_thread_once(
    ring: &mut IoUring,
    server: &TcpStream,
    ping_buffer: &mut Ping,
) -> anyhow::Result<bool> {
    let receive_pong = opcode::Read::new(
        types::Fd(server.as_raw_fd()),
        ping_buffer.as_bytes_mut().as_mut_ptr(),
        ping_buffer.as_bytes_mut().len() as u32,
    )
    .build();

    let mut submission = ring.submission();
    // Safety: the pointers never become dangling since we wait for submitted
    // entries even in case of an error-caused shirt circuit.
    unsafe {
        submission
            .push(&receive_pong)
            .context("SubmissionQueue::push")?;
    }
    drop(submission);
    ring.submit_and_wait(1)
        .context("IoUring::submit_and_wait")?;

    let read_done = ring.completion().next().context("Where's the result?")?;
    let bytes_read: u32 = read_done
        .result()
        .try_into()
        .context("Reading terminated with error")?;
    if bytes_read == 0 {
        return Ok(false);
    }

    let send_ping = opcode::Write::new(
        types::Fd(server.as_raw_fd()),
        ping_buffer.as_bytes().as_ptr(),
        ping_buffer.as_bytes().len() as u32,
    )
    .build();

    let mut submission = ring.submission();
    // Safety: the pointers never become dangling since we wait for submitted
    // entries even in case of an error-caused shirt circuit.
    unsafe {
        submission
            .push(&send_ping)
            .context("SubmissionQueue::push")?;
    }
    drop(submission);

    ring.submit_and_wait(1)
        .context("IoUring::submit_and_wait")?;
    // Do not care about completion results.
    ring.completion().for_each(drop);

    Ok(true)
}
