document.addEventListener("DOMContentLoaded", () => {
    let dashboardPayload = null;
    let activeTabId = null;

    function toneFromScore(score) {
        if (score >= 80) return "danger";
        if (score >= 60) return "caution";
        if (score <= 30) return "positive";
        return "neutral";
    }

    function safeArray(value) {
        return Array.isArray(value) ? value : [];
    }

    function setText(id, value) {
        const el = document.getElementById(id);
        if (el) {
            el.textContent = value;
        }
    }

    function setHtml(id, html) {
        const el = document.getElementById(id);
        if (el) {
            el.innerHTML = html;
        }
    }

    function renderHistory(points) {
        const svg = document.getElementById("history-chart");
        if (!svg) return;
        svg.innerHTML = "";

        if (!points.length) {
            return;
        }

        const width = 320;
        const height = 120;
        const padding = 12;
        const values = points.map((point) => Number(point.value));
        const min = Math.min(...values);
        const max = Math.max(...values);
        const span = Math.max(max - min, 1);

        const grid = document.createElementNS("http://www.w3.org/2000/svg", "path");
        grid.setAttribute("d", `M ${padding} ${height - padding} L ${width - padding} ${height - padding}`);
        grid.setAttribute("stroke", "rgba(81, 101, 117, 0.35)");
        grid.setAttribute("stroke-width", "1.5");
        grid.setAttribute("fill", "none");
        svg.appendChild(grid);

        const pathData = values.map((value, index) => {
            const x = padding + (index * (width - padding * 2)) / Math.max(values.length - 1, 1);
            const y = height - padding - ((value - min) / span) * (height - padding * 2);
            return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
        }).join(" ");

        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute("d", pathData);
        path.setAttribute("stroke", "#b4492d");
        path.setAttribute("stroke-width", "3");
        path.setAttribute("stroke-linecap", "round");
        path.setAttribute("stroke-linejoin", "round");
        path.setAttribute("fill", "none");
        svg.appendChild(path);
    }

    function renderHero(hero) {
        setText("hero-title", hero.title || "Bottom Sniffer Dashboard");
        setText("hero-subtitle", hero.subtitle || "");
        setText("hero-headline", hero.headline || "");
        setText("hero-score", String(hero.score ?? "--"));
        setText("hero-updated", `Updated ${hero.updated_at || "--"}`);
        setText("hero-confidence", `Confidence ${hero.confidence || "--"}`);
        setText("hero-quality", `Data quality ${hero.data_quality || "--"}`);

        const regimeEl = document.getElementById("hero-regime");
        if (regimeEl) {
            const tone = toneFromScore(Number(hero.score || 0));
            regimeEl.className = `pill pill-${tone}`;
            regimeEl.textContent = hero.regime || "--";
        }

        setHtml(
            "hero-drivers",
            safeArray(hero.drivers)
                .map((driver) => `<span class="driver-chip">${driver}</span>`)
                .join(""),
        );

        renderHistory(safeArray(hero.history));

        const preview = hero.preview || {};
        setText("preview-assessment", preview.assessment || "Preview unavailable");
        setText("preview-session", preview.session || "--");
        setText("preview-financials", preview.financials || "--");
        setText("preview-loans", preview.loans || "--");
        setText("preview-jpy", preview.jpy || "--");
        setText("preview-kre", preview.regional_banks || "--");
    }

    function renderTriggers(items) {
        setHtml(
            "trigger-map",
            safeArray(items).map((item) => `
                <div class="trigger-item trigger-${item.tone || "neutral"}">
                    <strong>${item.label}</strong>
                    <span class="trigger-status">${item.status}</span>
                </div>
            `).join(""),
        );
    }

    function renderLeadership(leadership) {
        const renderList = (items) => safeArray(items).map((item) => `
            <div class="leader-item">
                <strong>${item.label}</strong>
                <div class="leader-value">${item.value}</div>
                <div class="leader-detail">${item.detail}</div>
            </div>
        `).join("");

        setHtml("leaders-list", renderList((leadership || {}).leaders));
        setHtml("laggards-list", renderList((leadership || {}).laggards));
    }

    function renderMethodology(items) {
        setHtml(
            "methodology-list",
            safeArray(items).map((item) => `<div class="methodology-item">${item}</div>`).join(""),
        );
    }

    function renderTabs() {
        const tabs = safeArray((dashboardPayload || {}).tabs);
        if (!tabs.length) {
            setHtml("tab-buttons", "");
            setHtml("tab-cards", `<div class="error-card">No landing sections were returned.</div>`);
            return;
        }

        if (!activeTabId || !tabs.some((tab) => tab.id === activeTabId)) {
            activeTabId = tabs[0].id;
        }

        setHtml(
            "tab-buttons",
            tabs.map((tab) => `
                <button class="tab-btn ${tab.id === activeTabId ? "active" : ""}" data-tab-id="${tab.id}">
                    ${tab.label}
                </button>
            `).join(""),
        );

        const activeTab = tabs.find((tab) => tab.id === activeTabId) || tabs[0];
        setText("tab-summary", activeTab.description || "");
        setHtml(
            "tab-cards",
            safeArray(activeTab.cards).map((card) => `
                <article class="metric-card metric-${card.tone || "neutral"}">
                    <div class="metric-header">
                        <h3 class="metric-label">${card.label}</h3>
                        ${card.state ? `<span class="metric-state">${card.state}</span>` : ""}
                    </div>
                    <div class="metric-value">${card.value || "--"}</div>
                    <div class="metric-secondary">${card.secondary || ""}</div>
                    <div class="metric-note">${card.note || ""}</div>
                </article>
            `).join(""),
        );

        document.querySelectorAll("[data-tab-id]").forEach((button) => {
            button.addEventListener("click", () => {
                activeTabId = button.getAttribute("data-tab-id");
                renderTabs();
            });
        });
    }

    async function loadLanding() {
        try {
            const response = await fetch("/api/landing");
            const payload = await response.json();
            if (!response.ok) {
                throw new Error(payload.error || `HTTP ${response.status}`);
            }

            dashboardPayload = payload;
            renderHero(payload.hero || {});
            renderTriggers(payload.trigger_map || []);
            renderLeadership(payload.leadership || {});
            renderMethodology(payload.methodology || []);
            renderTabs();
        } catch (error) {
            setText("hero-headline", `Landing page unavailable: ${error.message}`);
            setHtml("tab-cards", `<div class="error-card">${error.message}</div>`);
        }
    }

    loadLanding();
    setInterval(loadLanding, 60000);
});
