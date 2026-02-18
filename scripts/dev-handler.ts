#!/usr/bin/env bun

import { $ } from "bun";
import { fileURLToPath } from "node:url";

function usage(): never {
  console.error(
    "Usage: bun run scripts/dev-handler.ts <cargo_package> <handler_id> [--debug] [--no-reload]",
  );
  console.error("Example: bun run scripts/dev-handler.ts ohlc_handler ohlc-1m");
  process.exit(1);
}

const args = process.argv.slice(2);
if (args.length < 2) {
  usage();
}

const cargoPackage = args[0]!;
const handlerId = args[1]!;
const flags = args.slice(2);

let profile: "release" | "dev" = "release";
let doReload = true;

for (const flag of flags) {
  if (flag === "--debug") {
    profile = "dev";
    continue;
  }
  if (flag === "--no-reload") {
    doReload = false;
    continue;
  }
  console.error(`Unknown arg: ${flag}`);
  usage();
}

const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const pgurl = process.env.PGURL ?? "postgresql:///postgres";
let handlersDir = process.env.CHAINSYNC_HANDLERS_DIR;

if (!handlersDir) {
  try {
    const dataDir = (
      await $`psql ${pgurl} -At -c "SHOW data_directory;"`
        .quiet()
        .text()
    ).trim();
    if (dataDir) {
      handlersDir = `${dataDir}/chainsync/handlers`;
    }
  } catch {
    // Keep compatibility with local pgrx dev if PG is not reachable yet.
    handlersDir = "/home/jk/.pgrx/18.2/handlers";
  }
}
handlersDir ??= "/home/jk/.pgrx/18.2/handlers";

const libStem = process.env.HANDLER_LIB_STEM ?? cargoPackage.replaceAll("-", "_");
const sourceSo =
  profile === "release"
    ? `${repoRoot}target/release/lib${libStem}.so`
    : `${repoRoot}target/debug/lib${libStem}.so`;

const destDir = `${handlersDir}/${handlerId}`;
const destSo = `${destDir}/handler.so`;

console.log(`[dev-handler] building package=${cargoPackage} profile=${profile}`);
if (profile === "release") {
  await $`cargo build --release -p ${cargoPackage}`.cwd(repoRoot);
} else {
  await $`cargo build -p ${cargoPackage}`.cwd(repoRoot);
}

if (!(await Bun.file(sourceSo).exists())) {
  throw new Error(`[dev-handler] build finished but missing ${sourceSo}`);
}

await $`mkdir -p ${destDir}`;
await $`cp ${sourceSo} ${destSo}`;
console.log(`[dev-handler] deployed ${destSo}`);

if (doReload) {
  console.log(`[dev-handler] reloading chainsync via ${pgurl}`);
  try {
    await $`psql ${pgurl} -v ON_ERROR_STOP=1 -c "SELECT chainsync.reload();"`.quiet();
  } catch {
    const preload = await $`psql ${pgurl} -At -c "SHOW shared_preload_libraries;"`.text();
    const ext = await $`psql ${pgurl} -At -c "SELECT 1 FROM pg_extension WHERE extname = 'pg_chainsync';"`.text();
    const hasPreload = preload.includes("pg_chainsync");
    const hasExt = ext.trim() === "1";

    if (!hasPreload) {
      throw new Error(
        [
          "[dev-handler] reload failed: pg_chainsync is not preloaded.",
          "Set shared_preload_libraries='pg_chainsync', restart postgres, then run CREATE EXTENSION pg_chainsync; once.",
        ].join(" "),
      );
    }

    if (!hasExt) {
      console.log(
        "[dev-handler] extension pg_chainsync is missing, creating it now",
      );
      await $`psql ${pgurl} -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS pg_chainsync;"`.quiet();
      await $`psql ${pgurl} -v ON_ERROR_STOP=1 -c "SELECT chainsync.reload();"`.quiet();
      console.log("[dev-handler] reload complete");
      process.exit(0);
    }

    throw new Error(
      "[dev-handler] reload failed although preload and extension exist. Check postgres logs for details.",
    );
  }
  console.log("[dev-handler] reload complete");
}
