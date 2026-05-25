/**
 * WebSocket streaming transcription test.
 * Converts sample_swedish.mp3 to PCM16 via ffmpeg, streams it to
 * ws://localhost:8787/v1/stream in 100ms chunks, and prints all messages.
 *
 * Usage:  node test/ws-stream-test.js [--lang en|sv]
 */
import { createReadStream } from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(import.meta.url);
const WebSocket = require("ws");

const SAMPLE = fileURLToPath(
  new URL("./fixtures/sample_swedish.mp3", import.meta.url),
);

const LANG = process.argv.includes("--lang")
  ? process.argv[process.argv.indexOf("--lang") + 1]
  : "sv";

const WS_URL = "ws://localhost:8787/v1/stream";
const CHUNK_BYTES = 16000 * 2 * 0.1; // 100ms of PCM16 at 16kHz

// Convert MP3 → raw PCM16 LE 16kHz mono via ffmpeg
function decodeAudioToPcm16(filePath) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const ff = spawn("ffmpeg", [
      "-i", filePath,
      "-ar", "16000",
      "-ac", "1",
      "-f", "s16le",
      "-",
    ], { stdio: ["ignore", "pipe", "ignore"] });

    ff.stdout.on("data", (chunk) => chunks.push(chunk));
    ff.stdout.on("end", () => resolve(Buffer.concat(chunks)));
    ff.on("error", reject);
    ff.on("close", (code) => {
      if (code !== 0 && chunks.length === 0) {
        reject(new Error(`ffmpeg exited with code ${code}`));
      }
    });
  });
}

async function run() {
  console.log(`[test] Decoding ${path.basename(SAMPLE)} to PCM16 ...`);
  const pcm16 = await decodeAudioToPcm16(SAMPLE);
  console.log(
    `[test] PCM16 ready: ${pcm16.length} bytes (${(pcm16.length / (16000 * 2)).toFixed(1)}s audio)`,
  );

  const ws = new WebSocket(WS_URL);

  ws.on("error", (err) => {
    console.error("[test] WebSocket error:", err.message);
    console.error(
      "[test] Is the server running?  npm run dev  in another terminal.",
    );
    process.exit(1);
  });

  ws.on("open", () => {
    console.log(`[test] Connected to ${WS_URL}`);
    // Send config first
    ws.send(JSON.stringify({ language: LANG, country: "SE", diarize: false }));
  });

  let messageCount = 0;
  const received = [];

  ws.on("message", (raw) => {
    const msg = JSON.parse(String(raw));
    messageCount++;

    if (msg.type === "ready") {
      console.log(`[test] Server ready, provider: ${msg.provider}`);
      // Start streaming audio in chunks
      streamAudio(ws, pcm16);
    } else if (msg.type === "transcript") {
      const flag = msg.isFinal ? "FINAL" : "interim";
      console.log(`[transcript:${flag}] ${msg.text}`);
      received.push(msg);
    } else if (msg.type === "utterance_end") {
      console.log("[utterance_end]");
    } else if (msg.type === "session_end") {
      console.log("\n─── SESSION END ──────────────────────────────────────────");
      console.log("Full transcript:");
      console.log(msg.fullTranscript || "(empty)");
      console.log(`Audio duration: ${msg.audioDurationSec?.toFixed(1)}s`);
      console.log(`Utterances: ${msg.utteranceCount}`);
      console.log("─────────────────────────────────────────────────────────\n");
      console.log(`[test] Done. Received ${messageCount} messages total.`);
      ws.close();
    } else if (msg.type === "error") {
      console.error("[server error]", msg.message);
    } else {
      console.log("[msg]", msg);
    }
  });

  ws.on("close", () => {
    console.log("[test] WebSocket closed.");
  });
}

function streamAudio(ws, pcm16) {
  const totalChunks = Math.ceil(pcm16.length / CHUNK_BYTES);
  let chunkIdx = 0;

  console.log(
    `[test] Streaming ${totalChunks} chunks (100ms each) ...`,
  );

  const interval = setInterval(() => {
    if (chunkIdx >= totalChunks) {
      clearInterval(interval);
      console.log("[test] Audio stream complete, sending stop ...");
      ws.send(JSON.stringify({ type: "stop" }));
      return;
    }

    const start = chunkIdx * CHUNK_BYTES;
    const end = Math.min(start + CHUNK_BYTES, pcm16.length);
    const chunk = pcm16.slice(start, end);

    if (ws.readyState === WebSocket.OPEN) {
      ws.send(chunk, { binary: true });
    } else {
      clearInterval(interval);
    }

    chunkIdx++;
  }, 100); // stream at real-time speed (100ms interval = 100ms audio)
}

run().catch((err) => {
  console.error("[test] Fatal:", err);
  process.exit(1);
});
