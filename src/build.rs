fn main() {
    let git_hash = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or("unknown".to_string());

    let build_type = if std::env::var("DEBUG").unwrap_or_default() == "true" {
        "debug"
    } else {
        "release"
    };

    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
    println!("cargo:rustc-env=BUILD_TYPE={}", build_type);
}
