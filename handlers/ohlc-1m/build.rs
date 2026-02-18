use std::env;
use std::fs;
use std::path::PathBuf;

fn resolve_env(input: &str) -> Result<(String, Vec<String>), String> {
    let mut vars = Vec::new();
    let mut out = String::with_capacity(input.len());

    for line in input.lines() {
        if line.trim_start().starts_with('#') {
            out.push_str(line);
            out.push('\n');
            continue;
        }

        let mut index = 0usize;
        while let Some(start_rel) = line[index..].find("${") {
            let start = index + start_rel;
            out.push_str(&line[index..start]);
            let Some(end_rel) = line[start + 2..].find('}') else {
                return Err("unterminated env placeholder in handler.toml".into());
            };
            let end = start + 2 + end_rel;
            let key = &line[start + 2..end];
            vars.push(key.to_string());
            let value = env::var(key).unwrap_or_else(|_| {
                println!(
                    "cargo:warning=missing env var {}, using empty string",
                    key
                );
                String::new()
            });
            out.push_str(&value);
            index = end + 1;
        }
        out.push_str(&line[index..]);
        out.push('\n');
    }
    Ok((out, vars))
}

fn main() {
    println!("cargo:rerun-if-changed=handler.toml");

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let handler_toml_path = manifest_dir.join("handler.toml");
    let source =
        fs::read_to_string(&handler_toml_path).expect("reading handler.toml");
    let (resolved, vars) = resolve_env(&source).expect("resolving env in handler.toml");

    vars.iter().for_each(|key| {
        println!("cargo:rerun-if-env-changed={}", key);
    });

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let output_path = out_dir.join("handler.generated.toml");
    fs::write(&output_path, resolved).expect("writing generated handler.toml");
}
