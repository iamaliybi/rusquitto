use std::io::{self};
use std::process::Command;

fn main() -> io::Result<()> {
	let mut child = Command::new("bash").arg("scripts/mosquitto.sh").spawn()?;

	let status = child.wait()?;

	if !status.success() {
		std::process::exit(status.code().unwrap_or(1));
	}

	Ok(())
}
