document.addEventListener("DOMContentLoaded", function () {
    const chartContainer = document.getElementById("chart-container");
    let chart = null;
    let scoreSeries = null;
    let latestPriceLine = null;

    if (window.LightweightCharts && chartContainer) {
        chart = LightweightCharts.createChart(chartContainer, {
            layout: {
                background: { color: "#101826" },
                textColor: "#9cb0cf",
            },
            grid: {
                vertLines: { color: "#2d3a52" },
                horzLines: { color: "#2d3a52" },
            },
            timeScale: { borderColor: "#2d3a52" },
            rightPriceScale: { borderColor: "#2d3a52" },
        });

        const areaOpts = {
            topColor: "rgba(222, 75, 75, 0.30)",
            bottomColor: "rgba(222, 75, 75, 0.00)",
            lineColor: "rgba(222, 75, 75, 1)",
            lineWidth: 2,
        };

        if (typeof chart.addAreaSeries === "function") {
            scoreSeries = chart.addAreaSeries(areaOpts);
        } else if (typeof chart.addSeries === "function" && window.LightweightCharts.AreaSeries) {
            scoreSeries = chart.addSeries(window.LightweightCharts.AreaSeries, areaOpts);
        } else {
            chartContainer.style.display = "none";
            chart = null;
        }
    } else if (chartContainer) {
        chartContainer.style.display = "none";
    }

    function envClass(color) {
        return `env-${color || "yellow"}`;
    }

    function esc(value) {
        if (value === null || value === undefined) return "";
        return String(value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;");
    }

    function formatDate(iso) {
        if (!iso) return "N/A";
        const d = new Date(iso);
        return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
    }

    function formatScore(value) {
        if (value === null || value === undefined || value === "") return "--";
        const num = Number(value);
        if (!Number.isFinite(num)) return "--";
        return String(Math.round(num));
    }

    function renderUnavailable(message) {
        const text = message || "Dashboard data unavailable.";
        document.getElementById("decision-banner").className = "section banner env-red";
        document.getElementById("env-label").textContent = "UNAVAILABLE";
        document.getElementById("headline-summary").textContent = text;
        document.getElementById("score-drivers").textContent = "Start Redis and run a structural update to populate dashboard data.";
        document.getElementById("composite-score").textContent = "--";
        document.getElementById("last-updated").textContent = "As of: N/A";

        const previewBadge = document.getElementById("preview-badge");
        previewBadge.textContent = "Intraday Preview: OFF";
        previewBadge.className = "badge preview-badge preview-off";

        const integrityBadge = document.getElementById("integrity-badge");
        integrityBadge.textContent = "Data unavailable";
        integrityBadge.className = "badge integrity-lagging";

        const confidenceBadge = document.getElementById("confidence-badge");
        confidenceBadge.textContent = "Confidence N/A";
        confidenceBadge.className = "badge env-red";

        const reliableBadge = document.getElementById("reliable-badge");
        reliableBadge.textContent = "Reliable No";
        reliableBadge.className = "badge env-red";

        const lowConfidence = document.getElementById("low-confidence-warning");
        lowConfidence.style.display = "block";
        lowConfidence.textContent = text;

        document.getElementById("preview-as-of").textContent = "N/A";
        document.getElementById("preview-grid").innerHTML = "";
        document.getElementById("preview-assessment").textContent = "Preview suggests: Unavailable";
        document.getElementById("spillover-headline").textContent = "Unavailable";
        document.getElementById("spillover-detail").textContent = "No structural snapshot is available.";
        document.getElementById("spillover-checks").innerHTML = "";
        document.getElementById("positioning-list").innerHTML = "";
        document.getElementById("systemic-trigger-list").innerHTML = "";
        document.getElementById("chart-latest-score").textContent = "--";
        document.getElementById("chart-history-last").textContent = "--";
        document.getElementById("chart-as-of").textContent = "N/A";
        document.getElementById("history-lag-label").textContent = "";
        document.getElementById("components-as-of").textContent = "N/A";
        document.getElementById("indicator-grid").innerHTML = "";
        document.getElementById("last-alert-timestamp").textContent = "None";
        document.getElementById("last-alert-score").textContent = "N/A";
        document.getElementById("last-alert-cooldown").textContent = "N/A";
        document.getElementById("last-alert-reasons").innerHTML = "<li>No alert data available.</li>";
    }

    function classFromTone(tone) {
        if (tone === "positive") return "tone-positive";
        if (tone === "defensive") return "tone-defensive";
        return "tone-caution";
    }

    function updateBanner(data) {
        const banner = data.regime_banner || {};
        const envLabel = banner.label || data.environment?.label || "--";
        const envColor = banner.color || data.environment?.color || "yellow";
        const drivers = Array.isArray(banner.drivers) ? banner.drivers : [];

        document.getElementById("decision-banner").className = `section banner ${envClass(envColor)}`;
        document.getElementById("env-label").textContent = envLabel;
        document.getElementById("headline-summary").textContent = banner.summary || "Summary unavailable.";
        document.getElementById("score-drivers").textContent = drivers.length
            ? `Primary drivers: ${drivers.join(", ")}.`
            : "Primary drivers: no dominant stress factor.";
        document.getElementById("composite-score").textContent = formatScore(banner.score ?? data.composite_score);
        document.getElementById("last-updated").textContent = `As of: ${formatDate(data.as_of?.banner || data.computed_at_utc)}`;

        const confidence = banner.confidence || "N/A";
        const confEl = document.getElementById("confidence-badge");
        confEl.textContent = `Confidence ${confidence}`;
        confEl.className = `badge ${envClass(
            confidence === "HIGH" ? "green" : confidence === "MED" ? "yellow" : "red"
        )}`;

        const reliable = banner.reliable !== false;
        const relEl = document.getElementById("reliable-badge");
        relEl.textContent = `Reliable ${reliable ? "Yes" : "No"}`;
        relEl.className = `badge ${envClass(reliable ? "green" : "red")}`;

        const integrity = data.integrity || {};
        const integrityEl = document.getElementById("integrity-badge");
        integrityEl.textContent = integrity.badge_text || "Run integrity --";
        integrityEl.className = `badge ${
            integrity.consistent ? "integrity-consistent" : "integrity-lagging"
        }`;

        const lowConfidence = document.getElementById("low-confidence-warning");
        if (!reliable) {
            lowConfidence.style.display = "block";
            lowConfidence.textContent = "Low confidence: weighted component coverage is incomplete.";
        } else {
            lowConfidence.style.display = "none";
        }
    }

    function renderSpillover(data) {
        const spill = data.spillover_risk || {};
        document.getElementById("spillover-headline").textContent = spill.headline || "Unavailable";
        document.getElementById("spillover-headline").className = `spillover-headline ${envClass(spill.color || "green")}`;
        document.getElementById("spillover-detail").textContent = spill.detail || "";

        const checks = Array.isArray(spill.checks) ? spill.checks : [];
        document.getElementById("spillover-checks").innerHTML = checks.map((check) => `
            <div class="check-item ${check.triggered ? "triggered" : "clear"}">
                <span>${esc(check.label)}</span>
                <span class="check-state">${check.triggered ? "On" : "Off"}</span>
            </div>
        `).join("");
    }

    function renderPositioning(data) {
        const items = Array.isArray(data.positioning_guidance) ? data.positioning_guidance : [];
        document.getElementById("positioning-list").innerHTML = items.map((item) => `
            <div class="positioning-item">
                <div class="positioning-top">
                    <span class="positioning-bucket">${esc(item.bucket)}</span>
                    <span class="positioning-stance ${classFromTone(item.tone)}">${esc(item.stance)}</span>
                </div>
                <p>${esc(item.note)}</p>
            </div>
        `).join("");
    }

    function renderSystemicTriggers(data) {
        const items = Array.isArray(data.systemic_triggers) ? data.systemic_triggers : [];
        document.getElementById("systemic-trigger-list").innerHTML = items.map((item) => `
            <div class="trigger-item ${item.triggered ? "triggered" : "not-triggered"}">
                <strong>${esc(item.label)}</strong>
                <div class="rule">${esc(item.rule || "")}</div>
                <div class="state">${item.triggered ? "Triggered" : "Not Triggered"}</div>
            </div>
        `).join("");
    }

    function renderPreview(data) {
        const preview = data.intraday_preview || {};
        const badge = document.getElementById("preview-badge");
        const panel = document.getElementById("preview-panel");
        const asOf = document.getElementById("preview-as-of");
        const grid = document.getElementById("preview-grid");
        const assessment = document.getElementById("preview-assessment");

        const available = preview.available === true;
        const stale = preview.stale === true;
        badge.textContent = preview.badge_text || "Intraday Preview: OFF";
        badge.className = `badge preview-badge ${!available ? "preview-off" : stale ? "preview-stale" : "preview-on"}`;
        panel.className = `section ${stale ? "preview-dim" : ""}`.trim();

        asOf.textContent = formatDate(preview.computed_at_utc);
        assessment.textContent = preview.assessment
            ? `Preview suggests: ${preview.assessment}`
            : "Preview suggests: Unavailable";

        const components = preview.components || {};
        const order = ["loans", "financials", "jpy"];
        grid.innerHTML = order.map((id) => {
            const item = components[id] || {};
            const label = esc(item.label || id);
            const stateText = esc(item.state_text || "Unavailable");
            const level = esc(item.level || "unavailable");
            return `
                <div class="preview-item status-${level}">
                    <span class="preview-name">${label}</span>
                    <span class="preview-state">${stateText}</span>
                </div>
            `;
        }).join("");
    }

    function advancedMetricsHtml(id, details) {
        if (!details || details.data_missing) {
            return "<p>No advanced metrics.</p>";
        }

        const rows = [];
        if (id === "ig_spreads" || id === "hy_credit") {
            rows.push(`Latest: <span>${details.latest?.toFixed?.(2) ?? "--"}</span>`);
            rows.push(`5d: <span>${details.change_5d?.toFixed?.(2) ?? "--"}</span>`);
            rows.push(`20d: <span>${details.change_20d?.toFixed?.(2) ?? "--"}</span>`);
            rows.push(`60d: <span>${details.change_60d?.toFixed?.(2) ?? "--"}</span>`);
            rows.push(`Z-score: <span>${details.z_score_1y?.toFixed?.(2) ?? "--"}</span>`);
        } else if (id === "leveraged_loans") {
            rows.push(`Price vs 200DMA: <span>${((details.price_vs_200dma || 0) * 100).toFixed(2)}%</span>`);
            rows.push(`30d Drawdown: <span>${((details.drawdown_30d || 0) * 100).toFixed(2)}%</span>`);
            rows.push(`Vol Z-score: <span>${details.volatility_z_score?.toFixed?.(2) ?? "--"}</span>`);
        } else if (id === "xlf_spy" || id === "kre_spy") {
            rows.push(`Latest ratio: <span>${details.latest_ratio?.toFixed?.(3) ?? "--"}</span>`);
            rows.push(`Z-score: <span>${details.z_score_1y?.toFixed?.(2) ?? "--"}</span>`);
            rows.push(`vs 50DMA: <span>${((details.ratio_vs_ma50 || 0) * 100).toFixed(2)}%</span>`);
            rows.push(`vs 200DMA: <span>${((details.ratio_vs_ma200 || 0) * 100).toFixed(2)}%</span>`);
            rows.push(`Breakdown: <span>${details.breakdown_flag ? "Yes" : "No"}</span>`);
        } else if (id === "30y_yield") {
            rows.push(`Latest yield: <span>${details.latest_yield?.toFixed?.(2) ?? "--"}%</span>`);
            rows.push(`20d bps: <span>${details.dgs30_20d_bps?.toFixed?.(1) ?? "--"}</span>`);
            rows.push(`Signal: <span>${esc(details.dgs30_signal || "neutral")}</span>`);
            rows.push(`Below 200DMA: <span>${details.is_below_200dma ? "Yes" : "No"}</span>`);
        } else if (id === "jpy_risk") {
            rows.push(`5d move: <span>${details.move_5d_pct?.toFixed?.(2) ?? "--"}%</span>`);
            rows.push(`Vol percentile: <span>${details.vol_percentile_1y?.toFixed?.(1) ?? "--"}</span>`);
            rows.push(`USDJPY 20DMA: <span>${details.usdjpy_20dma?.toFixed?.(2) ?? "--"}</span>`);
            rows.push(`Confirmed: <span>${details.jpy_confirmed ? "Yes" : "No"}</span>`);
        }

        return rows.map((row) => `<p>${row}</p>`).join("");
    }

    function renderCards(data) {
        const cards = Array.isArray(data.component_cards) ? data.component_cards : [];
        document.getElementById("components-as-of").textContent = formatDate(data.as_of?.components);
        document.getElementById("indicator-grid").innerHTML = cards.map((card) => `
            <div class="indicator-card status-${esc(card.status?.level || "no_data")}">
                <div class="card-top">
                    <div>
                        <h3 class="indicator-title">${esc(card.label)}</h3>
                        <div class="status-line">
                            <span class="status-dot"></span>
                            <span>${esc(card.status?.text || "No Data")}</span>
                        </div>
                    </div>
                    <div class="subscore">Subscore: ${formatScore(card.subscore)}</div>
                </div>
                <p class="driver">${esc(card.driver || "")}</p>
                <p class="why">${esc(card.why_it_matters || "")}</p>
                <details class="advanced">
                    <summary>Advanced metrics</summary>
                    <div class="metrics">${advancedMetricsHtml(card.id, card.details || {})}</div>
                </details>
            </div>
        `).join("");
    }

    function renderChart(data) {
        const chartMeta = data.chart || {};
        const points = Array.isArray(chartMeta.points) ? chartMeta.points : [];
        const latestSnapshotScore = formatScore(chartMeta.latest_marker_score ?? data.composite_score);
        const integrity = data.integrity || {};
        const historyLastScore = integrity.history_last_score;
        const historyLag = integrity.history_lag_minutes;

        document.getElementById("chart-latest-score").textContent = latestSnapshotScore;
        document.getElementById("chart-history-last").textContent = historyLastScore === null || historyLastScore === undefined
            ? "--"
            : formatScore(historyLastScore);
        document.getElementById("chart-as-of").textContent = formatDate(data.as_of?.chart);

        const lagLabel = document.getElementById("history-lag-label");
        if (integrity.consistent) {
            lagLabel.className = "history-lag consistent";
            lagLabel.textContent = "Consistent run";
        } else {
            lagLabel.className = "history-lag lagging";
            lagLabel.textContent = `History lagging by ${historyLag ?? "?"} min`;
        }

        if (scoreSeries && chart && points.length > 0) {
            const chartData = points.map((point) => ({
                time: point.time,
                value: Number(point.value),
            }));
            scoreSeries.setData(chartData);
            chart.timeScale().fitContent();

            if (latestPriceLine && typeof scoreSeries.removePriceLine === "function") {
                scoreSeries.removePriceLine(latestPriceLine);
            }

            if (typeof scoreSeries.createPriceLine === "function") {
                latestPriceLine = scoreSeries.createPriceLine({
                    price: Number(chartMeta.latest_marker_score ?? data.composite_score),
                    color: "#e0b84f",
                    lineWidth: 1,
                    lineStyle: 2,
                    axisLabelVisible: true,
                    title: "Snapshot",
                });
            }
        }
    }

    function renderAlert(data) {
        const alert = data.alert_display || {};
        document.getElementById("last-alert-timestamp").textContent = formatDate(alert.timestamp);
        document.getElementById("last-alert-score").textContent = formatScore(alert.score_at_alert);
        document.getElementById("last-alert-cooldown").textContent = formatDate(alert.cooldown_until);

        const reasons = Array.isArray(alert.reasons) ? alert.reasons.slice(0, 3) : [];
        document.getElementById("last-alert-reasons").innerHTML = reasons.length
            ? reasons.map((reason) => `<li>${esc(reason)}</li>`).join("")
            : "<li>No recent alert reasons.</li>";
    }

    function updateDashboard() {
        fetch("/api/dashboard")
            .then((response) => response.json().then((data) => ({ ok: response.ok, data })))
            .then(({ ok, data }) => {
                if (!ok || data.error) {
                    renderUnavailable(data.error || "Dashboard API unavailable.");
                    return;
                }
                return data;
            })
            .then((data) => {
                if (!data) return;
                updateBanner(data);
                renderSpillover(data);
                renderPositioning(data);
                renderSystemicTriggers(data);
                renderPreview(data);
                renderCards(data);
                renderChart(data);
                renderAlert(data);
            })
            .catch((err) => {
                console.error("Failed to fetch dashboard:", err);
                renderUnavailable("Failed to fetch dashboard data.");
            });
    }

    updateDashboard();
    setInterval(updateDashboard, 5 * 60 * 1000);
});
