/* 多 agent 健康饮食助理 · 前端逻辑：上传、SSE 流式过程、报告渲染、画像与历史 */
"use strict";

const $ = (id) => document.getElementById(id);

// ---------- 工具函数 ----------
function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// ---------- 状态 ----------
let selectedFile = null;

// ---------- 初始化 ----------
async function init() {
  loadHealth();
  loadProfile();
  loadMeals();
}

async function loadHealth() {
  try {
    const r = await fetch("/api/health");
    const d = await r.json();
    $("mode-badge").textContent = d.llm_mock_mode ? "Mock 模式（演示数据）" : `真实模式 · ${d.vision_model}`;
  } catch (e) {
    $("mode-badge").textContent = "后端未连接";
  }
}

// ---------- 上传区 ----------
const dz = $("dropzone"), fi = $("file-input");
dz.addEventListener("click", () => fi.click());
dz.addEventListener("dragover", (e) => { e.preventDefault(); dz.classList.add("dragover"); });
dz.addEventListener("dragleave", () => dz.classList.remove("dragover"));
dz.addEventListener("drop", (e) => {
  e.preventDefault(); dz.classList.remove("dragover");
  if (e.dataTransfer.files.length) setFile(e.dataTransfer.files[0]);
});
fi.addEventListener("change", () => { if (fi.files.length) setFile(fi.files[0]); });

function setFile(f) {
  if (!f.type.startsWith("image/")) { alert("请选择图片文件"); return; }
  selectedFile = f;
  $("dz-hint").hidden = true;
  $("dz-name").textContent = `${f.name}（${(f.size / 1024).toFixed(0)} KB）`;
  const reader = new FileReader();
  reader.onload = (e) => {
    const img = $("dz-preview");
    img.src = e.target.result;
    img.hidden = false;
  };
  reader.readAsDataURL(f);
}

// ---------- 开始分析（SSE） ----------
$("analyze-btn").addEventListener("click", async () => {
  if (!selectedFile) { alert("请先上传图片"); return; }
  const btn = $("analyze-btn");
  btn.disabled = true;
  $("process").innerHTML = "";
  $("report-card").hidden = true;
  $("progress").hidden = false;

  const fd = new FormData();
  fd.append("file", selectedFile);
  fd.append("meal_type", $("meal-type").value);
  fd.append("constraint", $("constraint").value);

  const progress = $("progress").firstElementChild;
  const stages = ["plan", "vision", "nutrition", "health", "gather", "report"];
  let seen = 0;
  let lastReport = null;

  try {
    const resp = await fetch("/api/analyze/stream", { method: "POST", body: fd });
    if (!resp.ok) {
      const err = await resp.text();
      addProcess("error", `请求失败：${err}`);
      return;
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      // SSE 帧分隔符兼容：sse-starlette 用 \r\n\r\n，这里统一处理两种换行
      let idx = nextFrameIdx(buf);
      while (idx >= 0) {
        const isCrlf = buf[idx] === "\r";
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + (isCrlf ? 4 : 2));
        handleFrame(frame);
        idx = nextFrameIdx(buf);
      }
    }

    function nextFrameIdx(s) {
      const crlf = s.indexOf("\r\n\r\n");
      const lf = s.indexOf("\n\n");
      if (crlf === -1) return lf;
      if (lf === -1) return crlf;
      return Math.min(crlf, lf);
    }
  } catch (e) {
    addProcess("error", `连接中断：${e.message}`);
  } finally {
    btn.disabled = false;
    $("progress").hidden = true;
  }

  function handleFrame(frame) {
    let event = "message", data = "";
    for (const line of frame.split("\n")) {
      if (line.startsWith("event:")) event = line.slice(6).trim();
      if (line.startsWith("data:")) data += line.slice(5).trim();
    }
    // done 事件 data 为空是正常的，必须放行；其余事件无 data 则跳过
    if (!data && event !== "done") return;
    let obj = {};
    try { obj = JSON.parse(data); } catch (e) { obj = { raw: data }; }

    if (event === "task_started") {
      addProcess("info", `已接收图片，任务开始（${obj.meal_type || ""}）`);
      seen = 1;
    } else if (event === "plan") {
      addProcess("plan", `Supervisor 意图识别 → <b>${esc(obj.intent)}</b>${obj.constraint ? `，约束：${esc(obj.constraint)}` : ""}`);
      bump("plan");
    } else if (event === "finding") {
      if (obj.stage === "vision") {
        addProcess("vision", `食物识别 Agent（千问VL）识别出 <b>${(obj.foods || []).length}</b> 种食物：${esc((obj.foods || []).map(f => `${f.name}${f.amount_g ? " " + f.amount_g + "g" : ""}`).join("、"))}`);
        bump("vision");
      } else if (obj.stage === "nutrition") {
        addProcess("nutrition", `营养分析 Agent 汇总：约 <b>${fmt(obj.totals.total_kcal)}</b> 千卡，蛋白 ${fmt(obj.totals.protein)}g / 脂肪 ${fmt(obj.totals.fat)}g / 碳水 ${fmt(obj.totals.carbs)}g`);
        bump("nutrition");
      } else if (obj.stage === "health") {
        addProcess("health", `健康评估 Agent：评分 <b>${obj.score}</b> 分${obj.reasons?.length ? `，要点：${esc(obj.reasons.slice(0, 2).join("；"))}` : ""}`);
        bump("health");
      }
    } else if (event === "agent_status") {
      addProcess("info", obj.msg || "专家结果汇总中");
      bump("gather");
    } else if (event === "report") {
      lastReport = obj;
      bump("report");
    } else if (event === "error") {
      addProcess("error", obj.msg || obj.raw || "执行出错");
    } else if (event === "done") {
      if (obj.ok === false) { addProcess("error", "分析未完成，请检查服务配置或稍后重试"); return; }
      if (lastReport) { renderReport(lastReport); loadMeals(); }
      addProcess("done", "✅ 分析完成");
      progress.style.width = "100%";
    }
  }

  function bump(stage) {
    seen = Math.max(seen, stages.indexOf(stage) + 1);
    progress.style.width = Math.min(100, Math.round((seen / stages.length) * 100)) + "%";
  }
});

function addProcess(kind, html) {
  const box = $("process");
  const div = document.createElement("div");
  div.className = `p-step${kind === "error" ? " error" : ""}`;
  div.innerHTML = `<span class="dot"></span><span class="p-body">${html}</span><span class="p-time">${new Date().toLocaleTimeString()}</span>`;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}

function fmt(v) { return Number(v ?? 0).toFixed(1); }

// ---------- 报告渲染 ----------
function renderReport(report) {
  $("report-card").hidden = false;
  const box = $("report");
  const metrics = report.metrics || [];
  const isScore = (m) => m.key === "score" || m.key === "health_score" || (m.name || "").includes("健康评分");
  const scoreM = metrics.find(isScore);
  const score = scoreM ? Number(scoreM.value) : 0;
  const ring = score >= 80 ? "high" : score >= 60 ? "mid" : "low";

  let html = "";
  html += `<div class="report-score">
      <div class="score-ring ${ring}">${score}</div>
      <div class="report-summary">${esc(report.summary || "")}
        <div class="evidence" style="margin-top:6px;">${esc(report.title || "")} · ${esc((report.period && report.period.start) || "")}</div>
      </div>
    </div>`;

  // 附加要求逐项回应
  if ((report.constraint_answers || []).length) {
    html += `<div class="section"><h3>附加要求回应</h3>`;
    for (const a of report.constraint_answers || []) {
      html += `<div class="insight"><div class="t">${esc(a.question || "附加要求")}</div>${esc(a.answer || "")}</div>`;
    }
    html += `</div>`;
  }

  // 指标卡
  html += `<div class="metrics">`;
  for (const m of metrics) {
    if (isScore(m)) continue;
    html += `<div class="metric"><div class="m-name">${esc(m.name)}</div>
      <div class="m-value">${fmt(m.value)}<small> ${esc(m.unit || "")}</small></div></div>`;
  }
  html += `</div>`;

  // 图表（柱状对比）
  for (const ch of report.charts || []) {
    const groups = ch.x || [];
    const sIn = (ch.series || []).find((s) => s.name === "摄入");
    const sSug = (ch.series || []).find((s) => s.name !== "摄入") || {};
    const maxV = Math.max(1, ...(sIn ? sIn.data : []), ...((sSug.data || []) || []));
    html += `<div class="chart-box"><h3>${esc(ch.title)}</h3><div class="bars">`;
    groups.forEach((g, i) => {
      const inv = sIn ? Number(sIn.data[i] || 0) : 0;
      const sug = sSug.data ? Number(sSug.data[i] || 0) : 0;
      const h1 = Math.max(2, Math.round((inv / maxV) * 130));
      const h2 = Math.max(2, Math.round((sug / maxV) * 130));
      html += `<div class="bar-group">
          <div class="bar-pair">
            <div class="bar in" style="height:${h1}px">${inv ? fmt(inv) : ""}</div>
            <div class="bar sug" style="height:${h2}px">${sug ? fmt(sug) : ""}</div>
          </div>
          <div class="bar-x">${esc(g)}</div>
        </div>`;
    });
    html += `</div><div class="legend"><span><i class="in"></i>摄入</span><span><i class="sug"></i>本餐建议</span></div></div>`;
  }

  // 发现
  if ((report.insights || []).length) {
    html += `<div class="section"><h3>关键发现</h3>`;
    for (const it of report.insights || []) {
      html += `<div class="insight"><div class="t">${esc(it.title)}</div>${esc(it.content)}</div>`;
    }
    html += `</div>`;
  }

  // 建议
  if ((report.actions || []).length) {
    html += `<div class="section"><h3>行动建议</h3>`;
    for (const a of report.actions || []) {
      const cls = (a.priority || "").toLowerCase().startsWith("p1") ? "p1" : "p2";
      html += `<div class="action"><span class="tag ${cls}">${esc(a.priority || "P2")}</span>
        <span class="t">${esc(a.action)}</span>${a.target ? `（针对：${esc(a.target)}）` : ""}
        ${a.expected_impact ? `<div class="evidence">预期：${esc(a.expected_impact)}</div>` : ""}</div>`;
    }
    html += `</div>`;
  }

  // 证据
  if ((report.evidence || []).length) {
    html += `<div class="section"><h3>证据来源</h3>`;
    for (const e of report.evidence || []) {
      html += `<div class="evidence">· [${esc(e.id)}] ${esc(e.source)} — ${esc(e.tool)}</div>`;
    }
    html += `</div>`;
  }

  box.innerHTML = html;
  box.scrollIntoView({ behavior: "smooth", block: "start" });
}

// ---------- 画像 ----------
async function loadProfile() {
  try {
    const r = await fetch("/api/profile");
    if (!r.ok) return;
    const p = await r.json();
    $("p-gender").value = p.gender || "男";
    $("p-age").value = p.age ?? 25;
    $("p-height").value = p.height ?? 170;
    $("p-weight").value = p.weight ?? 65;
    $("p-goal").value = p.goal || "保持健康";
    $("p-activity").value = p.activity_level || "轻量运动";
  } catch (e) { /* 忽略 */ }
}

$("save-profile").addEventListener("click", async () => {
  const body = {
    gender: $("p-gender").value,
    age: Number($("p-age").value),
    height: Number($("p-height").value),
    weight: Number($("p-weight").value),
    goal: $("p-goal").value,
    activity_level: $("p-activity").value,
  };
  try {
    const r = await fetch("/api/profile", {
      method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
    });
    const d = await r.json();
    $("profile-toast").textContent = d.message || "已保存";
  } catch (e) {
    $("profile-toast").textContent = "保存失败";
  }
});

// ---------- 历史 ----------
async function loadMeals() {
  try {
    const r = await fetch("/api/meals?limit=15");
    if (!r.ok) return;
    const meals = await r.json();
    const box = $("meal-list");
    if (!meals.length) { box.innerHTML = `<div class="empty">还没有记录</div>`; return; }
    box.innerHTML = "";
    for (const m of meals) {
      const item = document.createElement("div");
      item.className = "meal-item";
      item.innerHTML = `
        <div>
          <div class="m-info">${esc(m.meal_type)} · ${fmt(m.total_kcal)} 千卡</div>
          <div class="m-meta">${esc(m.created_at)}</div>
        </div>
        <div style="display:flex;align-items:center;">
          <span class="m-score">${Math.round(m.score)}</span>
          <button class="mini m-del">删</button>
        </div>`;
      item.addEventListener("click", () => loadMeal(m.id));
      item.querySelector(".m-del").addEventListener("click", async (e) => {
        e.stopPropagation();
        await fetch(`/api/meals/${m.id}`, { method: "DELETE" });
        loadMeals();
      });
      box.appendChild(item);
    }
  } catch (e) { /* 忽略 */ }
}

async function loadMeal(id) {
  try {
    const r = await fetch(`/api/meals/${id}`);
    const m = await r.json();
    $("report-card").hidden = false;
    renderReport(m.report || {});
    const proc = $("process");
    proc.innerHTML = "";
    addProcess("info", `历史记录加载：${esc(m.meal_type)} · ${esc(m.created_at)}`);
    const foods = m.foods || [];
    if (foods.length) addProcess("vision", `识别食物：${esc(foods.map((f) => `${f.name}${f.amount_g ? " " + f.amount_g + "g" : ""}`).join("、"))}`);
  } catch (e) { /* 忽略 */ }
}

init();
