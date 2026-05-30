mod args;
mod validate;

pub use args::{Cli, ColorArg, ContainerStateArg, FilterOnArg, FormatArg, JqModeArg, TimestampArg};

pub(crate) use validate::parse_since;

#[cfg(test)]
mod tests;
