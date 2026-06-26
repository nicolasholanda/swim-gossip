"use strict";

const STRINGS = {
    title: "swim-gossip",
    alive: "alive",
    suspect: "suspect",
    dead: "dead",
    probe: "probe",
    gossip: "gossip",
    speed: "speed",
    play: "▶",
    pause: "⏸",
    loading: "loading events…",
    loaded: (n) => `${n} events`,
    loadError: (e) => `failed to load events: ${e}`,
    timeFormat: (cur, total) => `${cur.toFixed(2)}s / ${total.toFixed(2)}s`,
    convergence: (s) => `convergence: ${s.toFixed(2)}s`,
    convergencePending: "convergence: …",
    live: (n) => `live · ${n} events`,
    liveConnecting: (url) => `connecting to ${url}…`,
    liveError: "SSE disconnected",
};

const COLORS = {
    bg: "#1e293b",
    node: "#334155",
    nodeBorder: "#475569",
    label: "#f1f5f9",
    alive: "#4ade80",
    suspect: "#fbbf24",
    dead: "#ef4444",
    probe: "#60a5fa",
    gossip: "#c084fc",
    timeout: "#fb7185",
    indirect: "#5eead4",
};

const PROBE_TYPES = new Set(["PING", "ACK", "PING_REQ", "INDIRECT_ACK"]);
const GOSSIP_TYPES = new Set(["SUSPECT", "ALIVE", "DEAD"]);
const STATE_TYPES = new Set(["SUSPECT", "ALIVE", "DEAD", "LEAVE"]);
const EDGE_DURATION_MS = 600;

const canvas = document.getElementById("canvas");
const ctx = canvas.getContext("2d");
const playBtn = document.getElementById("play-pause");
const speedSel = document.getElementById("speed");
const scrubber = document.getElementById("scrubber");
const timeLbl = document.getElementById("time");
const statusData = document.getElementById("status-data");
const statusConv = document.getElementById("status-convergence");

document.getElementById("title").textContent = STRINGS.title;
document.getElementById("legend-alive").textContent = STRINGS.alive;
document.getElementById("legend-suspect").textContent = STRINGS.suspect;
document.getElementById("legend-dead").textContent = STRINGS.dead;
document.getElementById("legend-probe").textContent = STRINGS.probe;
document.getElementById("legend-gossip").textContent = STRINGS.gossip;
document.getElementById("label-speed").textContent = STRINGS.speed;
playBtn.textContent = STRINGS.play;
statusData.textContent = STRINGS.loading;
statusConv.textContent = STRINGS.convergencePending;

const state = {
    events: [],
    nodes: [],
    positions: new Map(),
    startWall: 0,
    endWall: 0,
    durationMs: 0,
    currentMs: 0,
    playing: false,
    speed: 1,
    lastFrame: 0,
    convergenceMs: null,
    live: false,
};

function colorForType(type) {
    if (type === "PING" || type === "ACK") return COLORS.probe;
    if (type === "PING_REQ") return COLORS.gossip;
    if (type === "INDIRECT_ACK") return COLORS.indirect;
    if (type === "TIMEOUT") return COLORS.timeout;
    if (type === "SUSPECT") return COLORS.suspect;
    if (type === "ALIVE") return COLORS.alive;
    if (type === "DEAD") return COLORS.dead;
    return COLORS.label;
}

async function loadEvents(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();
    const events = text.split(/\r?\n/)
        .filter(l => l.trim().length > 0)
        .map(l => JSON.parse(l));
    events.sort((a, b) => a.timestampMillis - b.timestampMillis);
    return events;
}

function discoverNodes(events) {
    const set = new Set();
    for (const e of events) {
        if (e.source) set.add(e.source);
        if (e.target) set.add(e.target);
    }
    return Array.from(set).sort();
}

function layoutNodes(nodes) {
    const positions = new Map();
    const cx = canvas.width / 2;
    const cy = canvas.height / 2;
    const radius = Math.min(canvas.width, canvas.height) * 0.36;
    nodes.forEach((id, i) => {
        const angle = (2 * Math.PI * i) / nodes.length - Math.PI / 2;
        positions.set(id, {
            x: cx + radius * Math.cos(angle),
            y: cy + radius * Math.sin(angle),
        });
    });
    return positions;
}

function stateAtTime(nodeId, absoluteMs) {
    let current = "ALIVE";
    for (const e of state.events) {
        if (e.timestampMillis > absoluteMs) break;
        if (e.target === nodeId && STATE_TYPES.has(e.type)) {
            if (e.type === "LEAVE") {
                current = "DEAD";
            } else {
                current = e.type;
            }
        }
    }
    return current;
}

function activeEdges(absoluteMs) {
    const window = EDGE_DURATION_MS;
    const out = [];
    for (const e of state.events) {
        const dt = absoluteMs - e.timestampMillis;
        if (dt < 0) break;
        if (dt > window) continue;
        if (e.source && e.target && e.source !== e.target) {
            out.push({ event: e, progress: dt / window });
        }
    }
    return out;
}

function computeConvergence(events) {
    const leave = events.find(e => e.type === "LEAVE");
    if (!leave) return null;
    const dead = events.filter(e => e.type === "DEAD" && e.target === leave.target);
    if (dead.length === 0) return null;
    const allDeadAt = Math.max(...dead.map(e => e.timestampMillis));
    return (allDeadAt - leave.timestampMillis);
}

function drawNode(id, pos, nodeState) {
    const r = 24;
    ctx.beginPath();
    ctx.arc(pos.x, pos.y, r, 0, 2 * Math.PI);
    ctx.fillStyle = COLORS.node;
    ctx.fill();
    ctx.lineWidth = 3;
    ctx.strokeStyle = (
        nodeState === "ALIVE" ? COLORS.alive :
        nodeState === "SUSPECT" ? COLORS.suspect :
        nodeState === "DEAD" ? COLORS.dead :
        COLORS.nodeBorder
    );
    ctx.stroke();

    ctx.fillStyle = COLORS.label;
    ctx.font = "600 13px system-ui, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(id, pos.x, pos.y);
}

function drawEdge(srcPos, dstPos, color, progress, dashed) {
    const dx = dstPos.x - srcPos.x;
    const dy = dstPos.y - srcPos.y;
    const len = Math.hypot(dx, dy);
    const ux = dx / len;
    const uy = dy / len;
    const margin = 28;
    const x1 = srcPos.x + ux * margin;
    const y1 = srcPos.y + uy * margin;
    const x2 = dstPos.x - ux * margin;
    const y2 = dstPos.y - uy * margin;

    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    if (dashed) {
        ctx.setLineDash([6, 5]);
    } else {
        ctx.setLineDash([]);
    }
    const alpha = 1 - progress;
    ctx.globalAlpha = Math.max(0.15, alpha);
    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x2, y2);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.globalAlpha = 1;

    const px = x1 + (x2 - x1) * (1 - progress);
    const py = y1 + (y2 - y1) * (1 - progress);
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(px, py, 4, 0, 2 * Math.PI);
    ctx.fill();
}

function render() {
    const t = state.startWall + state.currentMs;
    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    const edges = activeEdges(t);
    for (const { event, progress } of edges) {
        const srcPos = state.positions.get(event.source);
        const dstPos = state.positions.get(event.target);
        if (!srcPos || !dstPos) continue;
        const color = colorForType(event.type);
        const dashed = GOSSIP_TYPES.has(event.type) || event.type === "TIMEOUT";
        drawEdge(srcPos, dstPos, color, progress, dashed);
    }

    for (const id of state.nodes) {
        const pos = state.positions.get(id);
        drawNode(id, pos, stateAtTime(id, t));
    }
}

function updateTimeLabel() {
    timeLbl.textContent = STRINGS.timeFormat(state.currentMs / 1000, state.durationMs / 1000);
}

function tick(now) {
    if (!state.lastFrame) state.lastFrame = now;
    const dt = now - state.lastFrame;
    state.lastFrame = now;

    if (state.playing) {
        state.currentMs = Math.min(state.durationMs, state.currentMs + dt * state.speed);
        scrubber.value = String(Math.floor(state.currentMs));
        if (state.currentMs >= state.durationMs) {
            state.playing = false;
            playBtn.textContent = STRINGS.play;
        }
    }
    updateTimeLabel();
    render();
    requestAnimationFrame(tick);
}

function setup(events) {
    state.events = events;
    state.nodes = discoverNodes(events);
    state.positions = layoutNodes(state.nodes);
    state.startWall = events[0].timestampMillis;
    state.endWall = events[events.length - 1].timestampMillis;
    state.durationMs = state.endWall - state.startWall;
    state.currentMs = 0;
    scrubber.max = String(state.durationMs);
    scrubber.value = "0";
    state.convergenceMs = computeConvergence(events);
    statusConv.textContent = state.convergenceMs != null
        ? STRINGS.convergence(state.convergenceMs / 1000)
        : STRINGS.convergencePending;
    statusData.textContent = STRINGS.loaded(events.length);
    updateTimeLabel();
}

function appendLiveEvent(e) {
    if (state.events.length === 0) {
        state.startWall = e.timestampMillis;
    }
    state.events.push(e);
    state.endWall = e.timestampMillis;
    state.durationMs = Math.max(0, state.endWall - state.startWall);

    const prevCount = state.nodes.length;
    state.nodes = discoverNodes(state.events);
    if (state.nodes.length !== prevCount) {
        state.positions = layoutNodes(state.nodes);
    }

    scrubber.max = String(state.durationMs);
    state.convergenceMs = computeConvergence(state.events);
    if (state.convergenceMs != null) {
        statusConv.textContent = STRINGS.convergence(state.convergenceMs / 1000);
    }

    const wasAtEnd = state.currentMs >= state.durationMs - 200;
    if (wasAtEnd) {
        state.currentMs = state.durationMs;
        scrubber.value = String(state.currentMs);
    }

    statusData.textContent = STRINGS.live(state.events.length);
}

function startLive(url) {
    state.live = true;
    state.playing = true;
    playBtn.textContent = STRINGS.pause;
    statusData.textContent = STRINGS.liveConnecting(url);
    const es = new EventSource(url);
    es.onmessage = (ev) => {
        try {
            const e = JSON.parse(ev.data);
            appendLiveEvent(e);
        } catch (err) {
            console.warn("bad event", err);
        }
    };
    es.onerror = () => {
        statusData.textContent = STRINGS.liveError;
    };
}

playBtn.addEventListener("click", () => {
    if (state.events.length === 0) return;
    state.playing = !state.playing;
    playBtn.textContent = state.playing ? STRINGS.pause : STRINGS.play;
    if (state.playing && state.currentMs >= state.durationMs) state.currentMs = 0;
    state.lastFrame = 0;
});

speedSel.addEventListener("change", () => {
    state.speed = parseFloat(speedSel.value);
});

scrubber.addEventListener("input", () => {
    state.currentMs = parseInt(scrubber.value, 10);
    updateTimeLabel();
});

(async function start() {
    const params = new URLSearchParams(window.location.search);
    if (params.has("live")) {
        const liveUrl = params.get("live") || "http://localhost:8080/events";
        startLive(liveUrl);
    } else {
        try {
            const url = params.get("data") || "sample-events.jsonl";
            const events = await loadEvents(url);
            if (events.length === 0) throw new Error("no events");
            setup(events);
        } catch (e) {
            statusData.textContent = STRINGS.loadError(e.message || String(e));
        }
    }
    requestAnimationFrame(tick);
})();
