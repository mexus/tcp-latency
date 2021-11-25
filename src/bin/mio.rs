use std::{
    io::{ErrorKind, Read, Write},
    mem::size_of,
    net::Ipv6Addr,
    time::{Duration, Instant},
};

use anyhow::Context;
use hdrhistogram::Histogram;
use mio::{
    net::{TcpListener, TcpStream},
    Interest, Token,
};
use structopt::StructOpt;
use tcp_latency::{Args, Ping};
use tokio::io::ReadBuf;
use zerocopy::AsBytes;

const ACCEPT_TOKEN: Token = Token(0);
const CLIENT_TOKEN: Token = Token(1);
const SERVER_STREAM_TOKEN: Token = Token(2);

enum ServerState<'a> {
    Listening,
    Copying(ServerStream<'a>),
}

fn main() -> anyhow::Result<()> {
    let Args {
        single_thread,
        iterations,
    } = Args::from_args();

    let listener =
        TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0).into()).context("TcpListener::bind")?;
    let address = listener.local_addr().context("TcpListener::local_addr")?;
    let client = TcpStream::connect(address).context("connect")?;

    let histogram = Histogram::<u64>::new(2).context("Histogram::new")?;

    let f = if single_thread {
        println!("Running on the current thread");
        run_single_thread
    } else {
        println!("Running in multiple threads");
        run_multi_thread
    };

    println!("Run {} instant ping-pongs", iterations);
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
    mut listener: TcpListener,
    mut client: TcpStream,
    mut histogram: Histogram<u64>,
    iterations: u32,
) -> anyhow::Result<Histogram<u64>> {
    let mut poll = mio::Poll::new().context("Poll::new")?;
    let mut events = mio::Events::with_capacity(3);

    poll.registry()
        .register(&mut listener, ACCEPT_TOKEN, Interest::READABLE)
        .context("Registering listener")?;

    poll.registry()
        .register(
            &mut client,
            CLIENT_TOKEN,
            Interest::WRITABLE | Interest::READABLE,
        )
        .context("Registering client")?;

    let mut client_ping_buffer = Ping::empty();
    let mut client = ClientStream::new(client, &mut client_ping_buffer, &mut histogram)
        .context("ClientStream::new")?;

    let mut server_ping_buffer = Ping::empty();
    let mut server_ping_buffer_ref = Some(&mut server_ping_buffer);
    let mut server_state = ServerState::Listening;

    while client.iteration() <= iterations {
        poll.poll(&mut events, None).context("poll")?;

        for event in &events {
            match event.token() {
                ACCEPT_TOKEN => {
                    // Server accept
                    server_state = match server_state {
                        ServerState::Listening => {
                            let (mut stream, _) = listener.accept().context("accept")?;
                            poll.registry()
                                .register(
                                    &mut stream,
                                    SERVER_STREAM_TOKEN,
                                    Interest::WRITABLE | Interest::READABLE,
                                )
                                .context("Register incoming stream")?;
                            let stream = ServerStream::new(
                                stream,
                                server_ping_buffer_ref
                                    .take()
                                    .context("Buffer already taken!")?,
                            );
                            ServerState::Copying(stream)
                        }
                        ServerState::Copying(_) => anyhow::bail!("Martian connection attempt"),
                    };
                }
                CLIENT_TOKEN | SERVER_STREAM_TOKEN => { /* Fallthrough */ }
                token => unreachable!("Unexpected token {:?}", token),
            }
        }

        match &mut server_state {
            ServerState::Listening => { /* The connection has not been accepted yet */ }
            ServerState::Copying(stream) => {
                client.run()?;
                stream.run()?;
            }
        };
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

    let histogram = client_handle
        .join()
        .expect("Client thread panicked")
        .context("Client thread terminated with an error")?;

    if let Err(e) = server_handle
        .join()
        .expect("Server thread panicked")
        .context("Server thread terminated with an error")
    {
        eprintln!("{:#}", e);
    }
    Ok::<_, anyhow::Error>(histogram)
}

fn client_thread(
    mut client: TcpStream,
    iterations: u32,
    mut histogram: Histogram<u64>,
) -> anyhow::Result<Histogram<u64>> {
    let mut poll = mio::Poll::new().context("Poll::new")?;
    let mut events = mio::Events::with_capacity(1);

    poll.registry()
        .register(
            &mut client,
            CLIENT_TOKEN,
            Interest::READABLE | Interest::WRITABLE,
        )
        .context("Client registration")?;

    let mut buffer = Ping::empty();
    let mut client =
        ClientStream::new(client, &mut buffer, &mut histogram).context("Client::new")?;

    while client.iteration() <= iterations {
        poll.poll(&mut events, None).context("Poll::poll")?;
        for _event in &events {
            // There's just one registered event.
            client.run()?
        }
    }

    Ok(histogram)
}

fn server_thread(mut listener: TcpListener) -> anyhow::Result<()> {
    let mut poll = mio::Poll::new().context("Poll::new")?;
    let mut events = mio::Events::with_capacity(2);
    poll.registry()
        .register(&mut listener, ACCEPT_TOKEN, Interest::READABLE)
        .context("Server registration")?;

    let mut server_state = ServerState::Listening;

    let mut ping_buffer = Ping::empty();
    let mut ping_buffer_ref = Some(&mut ping_buffer);

    loop {
        poll.poll(&mut events, None).context("Poll::poll")?;
        for event in &events {
            match event.token() {
                ACCEPT_TOKEN => {
                    anyhow::ensure!(
                        matches!(server_state, ServerState::Listening),
                        "Connection has already been accepted"
                    );
                    let (mut connection, _) = listener.accept().context("TcpListener::accept")?;
                    let ping_buffer = ping_buffer_ref
                        .take()
                        .context("The buffer has already been taken!")?;
                    poll.registry()
                        .register(
                            &mut connection,
                            SERVER_STREAM_TOKEN,
                            Interest::READABLE | Interest::WRITABLE,
                        )
                        .context("Registering a server stream")?;
                    server_state = ServerState::Copying(ServerStream::new(connection, ping_buffer));
                    continue;
                }
                SERVER_STREAM_TOKEN => { /* Fallthrough */ }
                token => unreachable!("Unexpected token {:?}", token),
            }
        }

        match &mut server_state {
            ServerState::Listening => { /* The connection has not been accepted yet */ }
            ServerState::Copying(stream) => {
                if stream.run()? {
                    // Connection is closed.
                    break;
                }
            }
        };
    }

    Ok(())
}

struct ServerStream<'a> {
    socket: TcpStream,
    buffer: &'a mut Ping,
    state: ServerStreamState<'a>,
}

impl<'a> ServerStream<'a> {
    fn new(socket: TcpStream, buffer: &'a mut Ping) -> Self {
        let state = ServerStreamState::Receiving(ReadBuf::new(unsafe {
            // Safety: this is safe since the underlying bytes will definitely
            // not outlive the "buffer" ping.
            &mut *(buffer.as_bytes_mut() as *mut [u8])
        }));
        Self {
            socket,
            buffer,
            state,
        }
    }
}

enum ServerStreamState<'a> {
    Receiving(ReadBuf<'a>),
    Sending { pong: Ping, bytes_sent: usize },
}

impl ServerStream<'_> {
    fn run(&mut self) -> anyhow::Result<bool> {
        loop {
            match &mut self.state {
                ServerStreamState::Receiving(buffer) => {
                    if buffer.filled().len() == size_of::<Ping>() {
                        // Ping received!
                        self.state = ServerStreamState::Sending {
                            pong: *self.buffer,
                            bytes_sent: 0,
                        };
                        continue;
                    }
                    let buf = buffer.initialize_unfilled();
                    let bytes = match self.socket.read(buf) {
                        Ok(0) => return Ok(true),
                        Ok(bytes) => bytes,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => anyhow::bail!("Server stream read error: {:#}", e),
                    };
                    buffer.advance(bytes);
                }
                ServerStreamState::Sending { pong, bytes_sent } => {
                    if *bytes_sent == size_of::<Ping>() {
                        // Pong sent!
                        self.state = ServerStreamState::Receiving(ReadBuf::new(unsafe {
                            // Safety: this is safe since the underlying bytes
                            // will definitely not outlive the "buffer" ping.
                            &mut *(self.buffer.as_bytes_mut() as *mut [u8])
                        }));
                        continue;
                    }
                    // Safety: we can't send more bytes than there are in the buffer.
                    let buf = unsafe { pong.as_bytes_mut().get_unchecked_mut(*bytes_sent..) };
                    let bytes = match self.socket.write(buf) {
                        Ok(bytes) => bytes,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => anyhow::bail!("Server stream write error: {:#}", e),
                    };
                    *bytes_sent += bytes;
                }
            }
        }
        Ok(false)
    }
}

struct ClientStream<'a> {
    socket: TcpStream,
    buffer: &'a mut Ping,
    state: ClientStreamState<'a>,
    iteration: u32,
    histogram: &'a mut Histogram<u64>,
}

impl<'a> ClientStream<'a> {
    pub fn new(
        socket: TcpStream,
        buffer: &'a mut Ping,
        histogram: &'a mut Histogram<u64>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            socket,
            buffer,
            state: ClientStreamState::Sending {
                ping: Ping::new().context("Ping::new")?,
                bytes_sent: 0,
            },
            iteration: 0,
            histogram,
        })
    }
}

enum ClientStreamState<'a> {
    Receiving(ReadBuf<'a>),
    Sending { ping: Ping, bytes_sent: usize },
}

impl ClientStream<'_> {
    fn iteration(&self) -> u32 {
        self.iteration
    }

    fn run(&mut self) -> anyhow::Result<()> {
        loop {
            match &mut self.state {
                ClientStreamState::Receiving(buffer) => {
                    if buffer.filled().len() == size_of::<Ping>() {
                        // Pong received!
                        let elapsed = self.buffer.elapsed().context("Ping::elapsed")?;
                        self.histogram
                            .record(elapsed as u64)
                            .context("Histogram::record")?;
                        self.iteration += 1;
                        self.state = ClientStreamState::Sending {
                            ping: Ping::new().context("Ping::new")?,
                            bytes_sent: 0,
                        };
                        continue;
                    }
                    let buf = buffer.initialize_unfilled();
                    let bytes = match self.socket.read(buf) {
                        Ok(bytes) => bytes,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => anyhow::bail!("Server stream read error: {:#}", e),
                    };
                    buffer.advance(bytes);
                }
                ClientStreamState::Sending { ping, bytes_sent } => {
                    if *bytes_sent == size_of::<Ping>() {
                        // Ping sent!
                        self.state = ClientStreamState::Receiving(ReadBuf::new(unsafe {
                            // Safety: this is safe since the underlying bytes
                            // will definitely not outlive the "buffer" ping.
                            &mut *(self.buffer.as_bytes_mut() as *mut [u8])
                        }));
                        continue;
                    }
                    // Safety: we can't send more bytes than there are in the buffer.
                    let buf = unsafe { ping.as_bytes_mut().get_unchecked_mut(*bytes_sent..) };
                    let bytes = match self.socket.write(buf) {
                        Ok(bytes) => bytes,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => anyhow::bail!("Server stream write error: {:#}", e),
                    };
                    *bytes_sent += bytes;
                }
            }
        }
        Ok(())
    }
}
