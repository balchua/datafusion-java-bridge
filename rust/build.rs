fn main() {
    // Capture the rustc version used to build the engine so the benchmark harness
    // can record it in its output.
    let rustc_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        // "rustc 1.98.1 (hash 2026-09-01)" -> "1.98.1" (keep the version segment only,
        // the benchmark harness splits the info string on spaces)
        .and_then(|s| s.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=DFE_RUSTC_VERSION={rustc_version}");
}
