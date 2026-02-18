#!/usr/bin/env bun

import { $ } from "bun";
import { readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";

type Mode =
  | { kind: "single"; cargoPackage: string; handlerId: string }
  | { kind: "by_handler_id"; handlerId: string }
  | { kind: "all" };

type HandlerSpec = {
  cargoPackage: string;
  handlerId: string;
};

function usage(): never {
  console.error(
    "Usage: bun run scripts/dev-handler.ts <cargo_package> <handler_id> [--debug] [--no-reload]",
  );
  console.error(
    "   or: bun run scripts/dev-handler.ts <handler_id> [--debug] [--no-reload]",
  );
  console.error(
    "   or: bun run scripts/dev-handler.ts --all [--debug] [--no-reload]",
  );
  console.error("Example: bun run scripts/dev-handler.ts ohlc_handler ohlc-1m");
  console.error("Example: bun run scripts/dev-handler.ts erc20-transfer");
  console.error("Example: bun run scripts/dev-handler.ts --all");
  process.exit(1);
}

function parseArgs(rawArgs: string[]): {
  mode: Mode;
  profile: "release" | "dev";
  doReload: boolean;
} {
  let profile: "release" | "dev" = "release";
  let doReload = true;

  const flags = rawArgs.filter((arg) => arg.startsWith("--"));
  const positional = rawArgs.filter((arg) => !arg.startsWith("--"));

  for (const flag of flags) {
    if (flag === "--debug") {
      profile = "dev";
      continue;
    }
    if (flag === "--no-reload") {
      doReload = false;
      continue;
    }
    if (flag === "--all") {
      continue;
    }
    console.error(`Unknown arg: ${flag}`);
    usage();
  }

  if (flags.includes("--all")) {
    if (positional.length > 0) {
      console.error("Do not pass positional args with --all");
      usage();
    }
    return { mode: { kind: "all" }, profile, doReload };
  }

  if (positional.length === 0) {
    return { mode: { kind: "all" }, profile, doReload };
  }

  if (positional.length === 1) {
    return {
      mode: {
        kind: "by_handler_id",
        handlerId: positional[0]!,
      },
      profile,
      doReload,
    };
  }

  if (positional.length !== 2) {
    usage();
  }

  return {
    mode: {
      kind: "single",
      cargoPackage: positional[0]!,
      handlerId: positional[1]!,
    },
    profile,
    doReload,
  };
}

function parsePackageName(cargoToml: string): string {
  const packageSection = cargoToml.match(/\[package\][\s\S]*?(?:\n\[|$)/);
  if (!packageSection) {
    throw new Error("missing [package] section");
  }
  const nameMatch = packageSection[0].match(/(?:^|\n)\s*name\s*=\s*"([^"]+)"/);
  if (!nameMatch) {
    throw new Error("missing package.name in Cargo.toml");
  }
  return nameMatch[1]!;
}

function parseHandlerId(handlerToml: string): string {
  const handlerSection = handlerToml.match(/\[handler\][\s\S]*?(?:\n\[|$)/);
  if (!handlerSection) {
    throw new Error("missing [handler] section");
  }
  const idMatch = handlerSection[0].match(/(?:^|\n)\s*id\s*=\s*"([^"]+)"/);
  if (!idMatch) {
    throw new Error("missing handler.id in handler.toml");
  }
  return idMatch[1]!;
}

async function discoverHandlers(repoRoot: string): Promise<HandlerSpec[]> {
  const handlersRoot = `${repoRoot}handlers`;
  const entries = await readdir(handlersRoot, { withFileTypes: true });
  const out: HandlerSpec[] = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const dir = `${handlersRoot}/${entry.name}`;
    const cargoTomlPath = `${dir}/Cargo.toml`;
    const handlerTomlPath = `${dir}/handler.toml`;

    if (!(await Bun.file(cargoTomlPath).exists())) {
      continue;
    }
    if (!(await Bun.file(handlerTomlPath).exists())) {
      continue;
    }

    const cargoToml = await Bun.file(cargoTomlPath).text();
    const handlerToml = await Bun.file(handlerTomlPath).text();
    const cargoPackage = parsePackageName(cargoToml);
    const handlerId = parseHandlerId(handlerToml);

    out.push({ cargoPackage, handlerId });
  }

  out.sort((a, b) => a.handlerId.localeCompare(b.handlerId));
  return out;
}

async function resolveHandlerById(
  repoRoot: string,
  handlerId: string,
): Promise<HandlerSpec> {
  const all = await discoverHandlers(repoRoot);
  const found = all.filter((h) => h.handlerId === handlerId);
  if (found.length === 0) {
    throw new Error(`[dev-handler] handler_id '${handlerId}' was not found in /handlers`);
  }
  if (found.length > 1) {
    throw new Error(
      `[dev-handler] handler_id '${handlerId}' is duplicated in /handlers`,
    );
  }
  return found[0]!;
}

async function deployOne(
  repoRoot: string,
  handlersDir: string,
  profile: "release" | "dev",
  spec: HandlerSpec,
): Promise<void> {
  const libStem = spec.cargoPackage.replaceAll("-", "_");
  const sourceSo =
    profile === "release"
      ? `${repoRoot}target/release/lib${libStem}.so`
      : `${repoRoot}target/debug/lib${libStem}.so`;
  const destSo = `${handlersDir}/${spec.handlerId}.so`;

  console.log(
    `[dev-handler] building package=${spec.cargoPackage} profile=${profile}`,
  );
  if (profile === "release") {
    await $`cargo build --release -p ${spec.cargoPackage}`.cwd(repoRoot);
  } else {
    await $`cargo build -p ${spec.cargoPackage}`.cwd(repoRoot);
  }

  if (!(await Bun.file(sourceSo).exists())) {
    throw new Error(`[dev-handler] build finished but missing ${sourceSo}`);
  }

  await $`mkdir -p ${handlersDir}`;
  await $`cp ${sourceSo} ${destSo}`;
  console.log(`[dev-handler] deployed ${destSo}`);
}

async function reloadChainsync(pgurl: string): Promise<void> {
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
      return;
    }

    throw new Error(
      "[dev-handler] reload failed although preload and extension exist. Check postgres logs for details.",
    );
  }
  console.log("[dev-handler] reload complete");
}

const { mode, profile, doReload } = parseArgs(process.argv.slice(2));

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
    handlersDir = "/home/jk/.pgrx/data-18/chainsync/handlers";
  }
}
handlersDir ??= "/home/jk/.pgrx/data-18/chainsync/handlers";

let specs: HandlerSpec[];
if (mode.kind === "single") {
  specs = [{ cargoPackage: mode.cargoPackage, handlerId: mode.handlerId }];
} else if (mode.kind === "by_handler_id") {
  specs = [await resolveHandlerById(repoRoot, mode.handlerId)];
} else {
  specs = await discoverHandlers(repoRoot);
  if (specs.length === 0) {
    throw new Error("[dev-handler] --all found no handlers in /handlers");
  }
}

for (const spec of specs) {
  await deployOne(repoRoot, handlersDir, profile, spec);
}

if (doReload) {
  await reloadChainsync(pgurl);
}
