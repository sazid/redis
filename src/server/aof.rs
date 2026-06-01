use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    time::{Duration, Instant},
};

use crate::config::FsyncPolicy;

pub(crate) struct Aof {
    file: std::fs::File,
    fsync_policy: FsyncPolicy,
    last_sync: Instant,
}

const FSYNC_INTERVAL: Duration = Duration::from_secs(1);

impl Aof {
    pub(crate) fn new(path: impl AsRef<Path>, fsync_policy: FsyncPolicy) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file,
            fsync_policy,
            last_sync: Instant::now(),
        })
    }

    pub(crate) fn append(&mut self, command: &[u8]) -> io::Result<()> {
        self.file.write_all(command)?;
        match self.fsync_policy {
            FsyncPolicy::Always => {
                self.file.sync_data()?;
                // No need to update `self.last_sync` for `Always` policy
            }
            FsyncPolicy::EverySec => {
                if self.last_sync.elapsed() >= FSYNC_INTERVAL {
                    self.file.sync_data()?;
                    self.last_sync = Instant::now();
                }
            }
            FsyncPolicy::No => (),
        };
        Ok(())
    }
}
