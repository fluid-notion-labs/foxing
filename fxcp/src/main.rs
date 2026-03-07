//! fxcp — Smart filesystem copy CLI (thin wrapper over fxcp_core::sync)
//!
//! This binary can also be obtained by symlinking foxingd as `fxcp`.

fn main() -> anyhow::Result<()> {
    fxcp_core::sync::cli_main()
}
