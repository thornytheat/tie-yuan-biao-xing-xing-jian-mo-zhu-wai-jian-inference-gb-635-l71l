/* batches.js — 中断批次续跑。续跑沿用原批次号与钉版规则，重复点击不新增结果。 */
(function () {
  function noticeNode() {
    return document.querySelector("[data-notice]");
  }

  function showNotice(text, tone) {
    const node = noticeNode();
    if (!node) {
      return;
    }
    node.textContent = text;
    node.dataset.tone = tone || "";
    node.hidden = false;
  }

  async function resume(button) {
    const batchId = button.dataset.batch;
    button.disabled = true;
    const original = button.textContent;
    button.textContent = "续跑中";
    try {
      const response = await fetch(`/api/batches/${batchId}/resume`, { method: "POST" });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        showNotice(`${batchId}：${body.message || response.status}`, "bad");
        button.disabled = false;
        button.textContent = original;
        return;
      }
      const changed = body.source_changed ? "（源文件已替换，按受理快照续跑）" : "";
      showNotice(`${batchId} 已从「${body.resumed_from.stage}」续跑完成 ${changed}`, "ok");
      window.setTimeout(() => window.location.reload(), 600);
    } catch (error) {
      showNotice(String(error.message), "bad");
      button.disabled = false;
      button.textContent = original;
    }
  }

  function bind() {
    document.querySelectorAll("[data-action=resume]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        resume(button);
      });
    });
  }

  document.addEventListener("DOMContentLoaded", bind);
})();
