#!/usr/bin/env bun

import { $ } from "bun";
import { readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";

type Mode =
  | { kind: "single"; cargoPackage: string; handlerId: string }
  | { kind: "by_handler_id"; handlerId: string }
  | { kind: "all" };

type PluginSpec = {
  cargoPackage: string;
  pluginName: string;
};

type HandlerConfig = {
  handlerId: string;
  pluginName: string;
  handlerTomlPath: string;
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

function parsePluginName(handlerToml: string): string {
  const handlerSection = handlerToml.match(/\[handler\][\s\S]*?(?:\n\[|$)/);
  if (!handlerSection) {
    throw new Error("missing [handler] section");
  }
  const pluginMatch = handlerSection[0].match(
    /(?:^|\n)\s*plugin\s*=\s*"([^"]+)"/,
  );
  if (!pluginMatch) {
    throw new Error("missing handler.plugin in handler.toml");
  }
  return pluginMatch[1]!;
}

async function discoverPlugins(repoRoot: string): Promise<PluginSpec[]> {
  const pluginsRoot = `${repoRoot}plugins`;
  const entries = await readdir(pluginsRoot, { withFileTypes: true });
  const out: PluginSpec[] = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const dir = `${pluginsRoot}/${entry.name}`;
    const cargoTomlPath = `${dir}/Cargo.toml`;

    if (!(await Bun.file(cargoTomlPath).exists())) {
      continue;
    }

    const cargoToml = await Bun.file(cargoTomlPath).text();
    const cargoPackage = parsePackageName(cargoToml);
    const pluginName = entry.name;

    out.push({ cargoPackage, pluginName });
  }

  out.sort((a, b) => a.pluginName.localeCompare(b.pluginName));
  return out;
}

async function discoverHandlerConfigs(
  repoRoot: string,
): Promise<HandlerConfig[]> {
  const handlersRoot = `${repoRoot}handlers`;
  const entries = await readdir(handlersRoot, { withFileTypes: true });
  const out: HandlerConfig[] = [];

  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".toml")) {
      continue;
    }
    const handlerTomlPath = `${handlersRoot}/${entry.name}`;
    const handlerToml = await Bun.file(handlerTomlPath).text();
    const handlerId = parseHandlerId(handlerToml);
    const pluginName = parsePluginName(handlerToml);
    out.push({ handlerId, pluginName, handlerTomlPath });
  }

  out.sort((a, b) => a.handlerId.localeCompare(b.handlerId));
  return out;
}

async function resolveHandlerConfigById(
  repoRoot: string,
  handlerId: string,
): Promise<HandlerConfig> {
  const all = await discoverHandlerConfigs(repoRoot);
  const found = all.filter((h) => h.handlerId === handlerId);
  if (found.length === 0) {
    throw new Error(
      `[dev-handler] handler_id '${handlerId}' was not found in /handlers`,
    );
  }
  if (found.length > 1) {
    throw new Error(
      `[dev-handler] handler_id '${handlerId}' is duplicated in /handlers`,
    );
  }
  return found[0]!;
}

async function buildAndDeployPlugin(
  repoRoot: string,
  pluginsDir: string,
  profile: "release" | "dev",
  spec: PluginSpec,
): Promise<void> {
  const libStem = spec.cargoPackage.replaceAll("-", "_");
  const sourceSo =
    profile === "release"
      ? `${repoRoot}target/release/lib${libStem}.so`
      : `${repoRoot}target/debug/lib${libStem}.so`;
  const destSo = `${pluginsDir}/${spec.pluginName}.so`;

  console.log(
    `[dev-handler] building plugin=${spec.pluginName} package=${spec.cargoPackage} profile=${profile}`,
  );
  if (profile === "release") {
    await $`cargo build --release -p ${spec.cargoPackage}`.cwd(repoRoot);
  } else {
    await $`cargo build -p ${spec.cargoPackage}`.cwd(repoRoot);
  }

  if (!(await Bun.file(sourceSo).exists())) {
    throw new Error(`[dev-handler] build finished but missing ${sourceSo}`);
  }

  await $`mkdir -p ${pluginsDir}`;
  await $`cp ${sourceSo} ${destSo}`;
  console.log(`[dev-handler] deployed ${destSo}`);
}

async function deployHandlerToml(
  handlersDir: string,
  config: HandlerConfig,
): Promise<void> {
  const destHandlerToml = `${handlersDir}/${config.handlerId}.toml`;
  await $`mkdir -p ${handlersDir}`;
  await $`cp ${config.handlerTomlPath} ${destHandlerToml}`;
  console.log(`[dev-handler] deployed ${destHandlerToml}`);
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
let chainsyncDir = process.env.CHAINSYNC_DIR;
if (!chainsyncDir) {
  try {
    const dataDir = (
      await $`psql ${pgurl} -At -c "SHOW data_directory;"`
        .quiet()
        .text()
    ).trim();
    if (dataDir) {
      chainsyncDir = `${dataDir}/chainsync`;
    }
  } catch {
    chainsyncDir = "/home/jk/.pgrx/data-18/chainsync";
  }
}
chainsyncDir ??= "/home/jk/.pgrx/data-18/chainsync";
const handlersDir = `${chainsyncDir}/handlers`;
const pluginsDir = `${chainsyncDir}/plugins`;

const pluginSpecs = await discoverPlugins(repoRoot);
const pluginByName = new Map(pluginSpecs.map((p) => [p.pluginName, p]));
const handlerConfigs = await discoverHandlerConfigs(repoRoot);
const handlerById = new Map(handlerConfigs.map((h) => [h.handlerId, h]));

let selectedHandlers: HandlerConfig[] = [];
const selectedPlugins = new Map<string, PluginSpec>();

if (mode.kind === "single") {
  const handlerConfig = await resolveHandlerConfigById(repoRoot, mode.handlerId);
  selectedHandlers = [handlerConfig];
  selectedPlugins.set(handlerConfig.pluginName, {
    cargoPackage: mode.cargoPackage,
    pluginName: handlerConfig.pluginName,
  });
} else if (mode.kind === "by_handler_id") {
  const handlerConfig = await resolveHandlerConfigById(repoRoot, mode.handlerId);
  const plugin = pluginByName.get(handlerConfig.pluginName);
  if (!plugin) {
    throw new Error(
      `[dev-handler] handler '${handlerConfig.handlerId}' references missing plugin '${handlerConfig.pluginName}' in /plugins`,
    );
  }
  selectedHandlers = [handlerConfig];
  selectedPlugins.set(plugin.pluginName, plugin);
} else {
  selectedHandlers = handlerConfigs;
  if (selectedHandlers.length === 0) {
    throw new Error("[dev-handler] --all found no handlers in /handlers");
  }
  for (const handlerConfig of selectedHandlers) {
    const plugin = pluginByName.get(handlerConfig.pluginName);
    if (!plugin) {
      throw new Error(
        `[dev-handler] handler '${handlerConfig.handlerId}' references missing plugin '${handlerConfig.pluginName}' in /plugins`,
      );
    }
    selectedPlugins.set(plugin.pluginName, plugin);
  }
}

for (const plugin of selectedPlugins.values()) {
  await buildAndDeployPlugin(repoRoot, pluginsDir, profile, plugin);
}
for (const handler of selectedHandlers) {
  await deployHandlerToml(handlersDir, handler);
}

if (doReload) {
  await reloadChainsync(pgurl);
}
