use std::process::ExitCode;

use rusquitto::config::{Cli, Config};

fn main() -> ExitCode {
	let cli = Cli::parse_args();

	let config = match Config::load(&cli.config) {
		Ok(config) => config,
		Err(e) => {
			eprintln!("rusquitto: {e}");
			return ExitCode::FAILURE;
		}
	};

	match rusquitto::run(config) {
		Ok(()) => ExitCode::SUCCESS,
		Err(e) => {
			eprintln!("rusquitto: fatal: {e}");
			ExitCode::FAILURE
		}
	}
}
