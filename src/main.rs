use std::process::ExitCode;

use proxy::{config, log, master, tls};

fn usage(prog: &str) {
    eprintln!(
        "uso: {prog} -c <config.toml> [-t] [-w N]\n  \
         -c  fichero de configuración TOML\n  \
         -t  solo valida la configuración y sale\n  \
         -w  fuerza el número de workers\n\
         Señales: SIGHUP recarga la configuración (también se recarga\n\
         automáticamente al modificar el fichero); SIGTERM/SIGINT para."
    );
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let prog = args.first().map(String::as_str).unwrap_or("proxy");
    let (mut path, mut test_only, mut force_workers) = (None, false, 0usize);
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "-c" => path = it.next().cloned(),
            "-t" => test_only = true,
            "-w" => force_workers = it.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "-h" | "--help" => {
                usage(prog);
                return ExitCode::SUCCESS;
            }
            _ => {
                usage(prog);
                return ExitCode::from(2);
            }
        }
    }
    let Some(path) = path else {
        usage(prog);
        return ExitCode::from(2);
    };
    let (mut cfg, text) = match config::load(&path).and_then(|(c, t)| tls::validate_config(&c).map(|_| (c, t))) {
        Ok(x) => x,
        Err(e) if e.starts_with("no se puede leer") => {
            eprintln!("{e}");
            return ExitCode::from(1);
        }
        Err(e) => {
            eprintln!("configuración inválida: {e}");
            return ExitCode::from(1);
        }
    };
    if test_only {
        println!(
            "configuración OK: {} frontends, {} backends, {} workers",
            cfg.frontends.len(),
            cfg.backends.len(),
            cfg.workers
        );
        return ExitCode::SUCCESS;
    }
    if (1..=128).contains(&force_workers) {
        cfg.workers = force_workers;
    }
    if let Err(e) = log::init(&cfg.log_file, cfg.log_level, "master") {
        eprintln!("{e}");
        return ExitCode::from(1);
    }
    let rc = master::master_run(&path, cfg, text);
    log::shutdown();
    ExitCode::from(rc as u8)
}
