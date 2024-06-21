macro_rules! setup {
    ($name:ident, $sz:expr) => {
        let _ = env_logger::builder().try_init();
        let mut $name = Cluster::new($sz, None)?;
        $name.start();
    };
    ($name:ident, $sz:expr, $noise:expr) => {
        let _ = env_logger::builder().try_init();
        let mut $name = Cluster::new($sz, Some($noise))?;
        $name.start();
    };
}

macro_rules! teardown {
    ($name:ident) => {
        $name.close();
    };
}
mod cluster;
mod state;
mod tests;
mod transport;
