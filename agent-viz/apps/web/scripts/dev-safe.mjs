import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const projectRoot = process.cwd();
const nextDir = path.join(projectRoot, ".next");
const cacheDir = path.join(projectRoot, "node_modules", ".cache");

function removeIfPresent(target) {
  if (fs.existsSync(target)) {
    fs.rmSync(target, { recursive: true, force: true });
  }
}

removeIfPresent(nextDir);
removeIfPresent(cacheDir);

const nextBin =
  process.platform === "win32"
    ? path.join(projectRoot, "node_modules", ".bin", "next.cmd")
    : path.join(projectRoot, "node_modules", ".bin", "next");

const child = spawn(nextBin, ["dev", ...process.argv.slice(2)], {
  cwd: projectRoot,
  stdio: "inherit",
  env: process.env,
});

function shutdown(signal) {
  if (!child.killed) child.kill(signal);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
