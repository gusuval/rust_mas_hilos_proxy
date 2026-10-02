//! Lanza la batería de integración (tests/integration/run.sh) con los
//! binarios que acaba de compilar cargo. Necesita curl, jq, socat, openssl
//! y wrk (opcional), así que va marcada como #[ignore]:
//!
//!   cargo test -- --ignored

use std::path::Path;
use std::process::Command;

#[test]
#[ignore]
fn integration_suite() {
    let bin_dir = Path::new(env!("CARGO_BIN_EXE_proxy")).parent().unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/run.sh");
    let status = Command::new(script).arg(bin_dir).status().expect("no se puede lanzar run.sh");
    assert!(status.success(), "la batería de integración ha fallado");
}
