#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const crateDir = resolve(scriptDir, "..");
const repoRoot = resolve(crateDir, "../..");
const pkgDir = resolve(crateDir, "pkg");

const packageName = process.env.NPM_PACKAGE_NAME || "zenoh-wasm";
const packageVersion =
  process.env.NPM_PACKAGE_VERSION || `${workspaceField("version")}-wasm.0`;
const repositoryUrl =
  process.env.NPM_REPOSITORY_URL ||
  `git+${workspaceField("repository").replace(/\.git$/, "")}.git`;

run("wasm-pack", [
  "build",
  crateDir,
  "--target",
  "web",
  "--out-dir",
  "pkg",
  "--release",
]);

mkdirSync(pkgDir, { recursive: true });
writePackageJson();
copyFileSync(resolve(crateDir, "README.md"), resolve(pkgDir, "README.md"));
copyFileSync(resolve(repoRoot, "LICENSE"), resolve(pkgDir, "LICENSE"));

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    env: process.env,
    stdio: "inherit",
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function writePackageJson() {
  const template = readFileSync(resolve(crateDir, "package.template.json"), "utf8");
  const rendered = template
    .replaceAll("__NPM_PACKAGE_NAME__", packageName)
    .replaceAll("__NPM_PACKAGE_VERSION__", packageVersion)
    .replaceAll("__REPOSITORY_URL__", repositoryUrl);
  const parsed = JSON.parse(rendered);
  writeFileSync(
    resolve(pkgDir, "package.json"),
    `${JSON.stringify(parsed, null, 2)}\n`,
  );
}

function workspaceField(name) {
  const cargoToml = readFileSync(resolve(repoRoot, "Cargo.toml"), "utf8");
  const workspacePackage = cargoToml.match(
    /\[workspace\.package\]([\s\S]*?)(?:\n\[|$)/,
  );
  if (!workspacePackage) {
    throw new Error("Cargo.toml is missing [workspace.package]");
  }
  const match = workspacePackage[1].match(
    new RegExp(`^${name}\\s*=\\s*"([^"]+)"`, "m"),
  );
  if (!match) {
    throw new Error(`Cargo.toml is missing workspace package field ${name}`);
  }
  return match[1];
}
