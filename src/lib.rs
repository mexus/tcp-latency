use std::time::Duration;

use nix::time::ClockId;
use structopt::StructOpt;
use zerocopy::{AsBytes, FromBytes};

#[derive(Debug, AsBytes, FromBytes, Clone, Copy)]
#[repr(C)]
pub struct Ping {
    timestamp: u128,
}

impl Ping {
    pub fn empty() -> Self {
        Self { timestamp: 0 }
    }

    pub fn new() -> nix::Result<Self> {
        let timestamp: Duration = nix::time::clock_gettime(ClockId::CLOCK_MONOTONIC)?.into();
        let timestamp = timestamp.as_nanos() as u128;
        Ok(Ping { timestamp })
    }

    pub fn elapsed(&self) -> nix::Result<u128> {
        let now = nix::time::clock_gettime(ClockId::CLOCK_MONOTONIC)?;
        let now = (now.tv_sec() as u128) * 1_000_000_000 + now.tv_nsec() as u128;
        Ok(now - self.timestamp)
    }
}

#[derive(StructOpt)]
pub struct Args {
    #[structopt(long = "single")]
    pub single_thread: bool,
    pub iterations: u32,
}
