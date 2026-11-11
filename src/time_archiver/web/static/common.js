/* 本地原生 ES6：模式切换与任务状态轻量轮询，无外部依赖。 */
(function () {
  "use strict";

  const mode = document.querySelector("#run-mode");
  const custom = document.querySelector("[data-custom-source]");
  function syncMode() {
    if (mode && custom) {
      custom.hidden = mode.value !== "custom";
    }
  }
  if (mode) {
    mode.addEventListener("change", syncMode);
    syncMode();
  }

  const refreshButton = document.querySelector("[data-refresh-tasks]");
  if (refreshButton) {
    refreshButton.addEventListener("click", function () { window.location.reload(); });
  }

  async function refreshTaskStatuses() {
    const rows = Array.from(document.querySelectorAll("tr[data-status]"));
    if (!rows.length) return;
    try {
      const response = await fetch("/api/tasks", { headers: { "Accept": "application/json" } });
      if (!response.ok) return;
      const payload = await response.json();
      const statuses = new Map((payload.tasks || []).map(t => [t.task_id, t.status]));
      let changed = false;
      rows.forEach(function (row) {
        const id = row.id.replace(/^task-/, "");
        const next = statuses.get(id);
        if (!next || row.dataset.status === next) return;
        row.dataset.status = next;
        const badge = row.querySelector(".status");
        if (badge) {
          badge.className = "status status-" + next;
          badge.textContent = next;
        }
        if (next === "completed" || next === "failed") changed = true;
      });
      if (changed) setTimeout(function () { window.location.reload(); }, 600);
    } catch (error) {
      /* 离线页面不引入错误提示资源，保留现有状态。 */
    }
  }

  if (document.querySelector("tr[data-status]")) {
    window.setInterval(refreshTaskStatuses, 10000);
  }
})();
