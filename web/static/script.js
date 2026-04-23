document.addEventListener("DOMContentLoaded", () => {

    const TABS = document.querySelectorAll(".tab-btn");
    TABS.forEach(btn => {
        btn.addEventListener("click", () => {
            TABS.forEach(t => t.classList.remove("active"));
            btn.classList.add("active");
            document.querySelectorAll(".tab-pane").forEach(p => p.classList.remove("active"));
            const target = btn.getAttribute("data-target");
            const pane = document.getElementById(target);
            if(pane) pane.classList.add("active");
        });
    });

    const auditToggle = document.getElementById("audit-toggle");
    const auditContent = document.getElementById("audit-content");
    auditToggle.addEventListener("click", () => {
        auditContent.classList.toggle("hidden");
        auditToggle.textContent = auditContent.classList.contains("hidden") 
            ? "RAW AUDIT DRILLDOWN [+]" 
            : "RAW AUDIT DRILLDOWN [-]";
    });

    function formatNum(val, decimals=2) {
        if(val === null || val === undefined) return "--";
        const num = Number(val);
        if(isNaN(num)) return val;
        return num.toFixed(decimals);
    }

    function formatTimeET(iso) {
        if(!iso) return "--";
        try {
            const d = new Date(iso);
            if(isNaN(d)) return iso;
            return d.toLocaleTimeString("en-US", {timeZone: "America/New_York", hour12: false}) + " ET";
        } catch(e) {
            return iso;
        }
    }

    function getEnvColor(score) {
        if(score <= 30) return "bg-green";
        if(score <= 60) return "bg-yellow";
        if(score <= 80) return "bg-orange";
        return "bg-red";
    }
    
    function getEnvTextColor(score) {
        if(score <= 30) return "color-green";
        if(score <= 60) return "color-yellow";
        if(score <= 80) return "color-orange";
        return "color-red";
    }

    async function fetchData() {
        try {
            const [healthRes, structRes, prevRes, ctxRes] = await Promise.all([
                fetch("/api/health"),
                fetch("/api/macro/latest"),
                fetch("/api/macro/preview"),
                fetch("/api/macro/context")
            ]);
            
            const health = healthRes.ok ? await healthRes.json() : null;
            const struct = structRes.ok ? await structRes.json() : null;
            const prev = prevRes.ok ? await prevRes.json() : null;
            const ctx = ctxRes.ok ? await ctxRes.json() : null;

            render(health, struct, prev, ctx);
            renderAudit({health, struct, prev, ctx});
        } catch (err) {
            console.error("Fetch error", err);
        }
    }

    function render(health, struct, prev, ctx) {
        // Status Strip
        const tsRegime = document.getElementById("ts-regime");
        const tsScore = document.getElementById("ts-score");
        if(struct && !struct.error) {
            const score = struct.composite_score;
            tsRegime.textContent = struct.regime_label || "UNKNOWN";
            tsRegime.className = `badge ${getEnvColor(score)}`;
            tsScore.textContent = score;
            tsScore.className = getEnvTextColor(score);
            document.getElementById("ts-struc-time").textContent = formatTimeET(struct.computed_at_utc);
            
            const conf = struct.state_confidence?.confidence_level || struct.confidence || "N/A";
            const q = struct.data_quality?.completeness_score !== undefined 
                ? `${Math.round(struct.data_quality.completeness_score * 100)}%` : "N/A";
            
            document.getElementById("ts-quality").textContent = q;
            document.getElementById("ts-conf").textContent = conf;
            
            const flags = [];
            if(struct.is_stale) flags.push("STRUC_STALE");
            if(struct.anomaly_flags) {
                if(struct.anomaly_flags.extreme_move) flags.push("EXT_MOVE");
                if(struct.anomaly_flags.volatility_spike) flags.push("VOL_SPIKE");
                if(struct.anomaly_flags.multi_asset_divergence) flags.push("DIVERGENCE");
            }
            if(struct.data_quality?.critical_missing) flags.push("MISSING_CRIT");
            
            const tsFlags = document.getElementById("ts-flags");
            if(flags.length > 0) {
                tsFlags.textContent = flags.join(" ");
                tsFlags.className = "color-orange";
            } else {
                tsFlags.textContent = "OK";
                tsFlags.className = "color-green";
            }
        }

        if(prev && !prev.error) {
            const pa = prev.preview_spillover_assessment || "N/A";
            const badge = document.getElementById("ts-preview");
            badge.textContent = pa;
            if(pa.includes("Credit confirming")) badge.className = "badge bg-red";
            else if(pa.includes("watch credit")) badge.className = "badge bg-orange";
            else badge.className = "badge bg-green";
            document.getElementById("ts-prev-time").textContent = formatTimeET(prev.computed_at_utc);
        } else {
            document.getElementById("ts-preview").textContent = "OFF";
            document.getElementById("ts-preview").className = "badge bg-muted";
            document.getElementById("ts-prev-time").textContent = "--";
        }

        // Structural Summary
        if(struct && !struct.error) {
            document.getElementById("ss-score").textContent = struct.composite_score;
            document.getElementById("ss-score").className = `big-score ${getEnvTextColor(struct.composite_score)}`;
            document.getElementById("ss-headline").textContent = struct.headline_summary || "--";
            document.getElementById("ss-regime").textContent = struct.regime || "--";
            document.getElementById("ss-drivers").textContent = (struct.primary_drivers || []).join(", ") || "None";
            
            let dText = "--";
            if(struct.delta && struct.delta.available) {
                dText = `${struct.delta.score_change > 0 ? '+' : ''}${struct.delta.score_change} pts`;
                if(struct.delta.regime_changed) dText += ` (Regime changed)`;
            }
            document.getElementById("ss-delta").textContent = dText;
        }

        // Preview Summary
        if(prev && !prev.error) {
            document.getElementById("ps-assessment").textContent = prev.preview_spillover_assessment || "--";
            let psHtml = "";
            const pc = prev.component_statuses || prev.components || {};
            for(const [k,v] of Object.entries(pc)) {
                let text = typeof v === 'object' ? (v.status || v.state?.text || JSON.stringify(v)) : v;
                psHtml += `<tr><td>${k}</td><td>${text}</td></tr>`;
            }
            document.getElementById("ps-components").innerHTML = psHtml || "<tr><td colspan='2'>No components</td></tr>";
            
            document.getElementById("ps-session").textContent = prev.session?.market_session || "--";
            let pdText = "--";
            if(prev.delta && prev.delta.available) {
                pdText = "Updated";
            }
            document.getElementById("ps-delta").textContent = pdText;
        }

        // Health
        if(health && !health.error) {
            document.getElementById("hl-struc-age").textContent = `${Math.round((health.structural_age_seconds || 0)/60)}m`;
            document.getElementById("hl-prev-age").textContent = `${Math.round((health.preview_age_seconds || 0)/60)}m`;
            document.getElementById("hl-struc-age").className = health.structural_stale ? "color-red" : "color-green";
            document.getElementById("hl-prev-age").className = health.preview_stale ? "color-red" : "color-green";
            
            // if struct is passed in, use execution from it
            if(struct && struct.execution) {
                document.getElementById("hl-exec").textContent = `${formatNum(struct.execution.total_seconds, 2)}s`;
            }
            if(struct && struct.data_quality) {
                document.getElementById("hl-missing-crit").textContent = struct.data_quality.critical_missing ? "YES" : "NO";
                document.getElementById("hl-missing-crit").className = struct.data_quality.critical_missing ? "color-red" : "color-green";
                document.getElementById("hl-missing-noncrit").textContent = struct.data_quality.noncritical_missing_count || "0";
            }
        }

        // Deltas
        const dList = document.getElementById("delta-list");
        let dHtml = "";
        if(struct && struct.delta && struct.delta.available) {
            if(struct.delta.regime_changed) {
                dHtml += `<li>Regime changed from <strong>${struct.delta.previous_regime}</strong> to <strong>${struct.delta.current_regime}</strong></li>`;
            }
            const stc = struct.delta.component_state_changes || {};
            for(const [k,v] of Object.entries(stc)) {
                dHtml += `<li>${k}: state changed ${v.previous} -> ${v.current}</li>`;
            }
            const ssc = struct.delta.component_subscore_changes || {};
            for(const [k,v] of Object.entries(ssc)) {
                dHtml += `<li>${k}: subscore ${v.previous} -> ${v.current}</li>`;
            }
        }
        if(!dHtml) dHtml = "<li>No significant structural deltas.</li>";
        dList.innerHTML = dHtml;

        // Market Context
        const mc = (ctx && ctx.market_context) ? ctx.market_context : ctx;
        if(mc && Object.keys(mc).length > 0 && !mc.error) {
            renderContext(mc);
        } else {
            document.getElementById("context-content").innerHTML = "<div class='panel-content'>Context data unavailable.</div>";
        }
    }

    function renderContext(mc) {
        const ctc = document.getElementById("context-content");
        ctc.innerHTML = "";
        
        const sections = [
            { id: "ctx-macro", key: "macro_rates", title: "Macro / Rates", headers: ["Series", "Value", "5d", "20d", "Z-Score", "State"] },
            { id: "ctx-credit", key: "credit_liquidity", title: "Credit / Liquidity", headers: ["Series", "Value", "Z-Score", "State", "Stretch"] },
            { id: "ctx-equity", key: "equity_index_state", title: "Equity Index", headers: ["Asset", "Return 5d", "Return 20d", "vs 200DMA", "Z-Score", "State", "Stretch"] },
            { id: "ctx-sectors", key: "sector_state", title: "Sectors", headers: ["Sector", "vs SPY 5d", "vs SPY 20d", "Z-Score", "Leadership"] },
            { id: "ctx-vol", key: "volatility_stress", title: "Volatility / Stress", headers: ["Asset", "Realized Vol", "Percentile", "State"] },
            { id: "ctx-flight", key: "flight_to_safety", title: "Flight to Safety", headers: ["Asset", "State", "Stretch", "Description"] },
            { id: "ctx-cross", key: "cross_asset_relationships", title: "Cross Asset", headers: ["Pair", "Ratio", "5d", "Z-Score", "State"] },
            { id: "ctx-breadth", key: "breadth_participation", title: "Breadth", headers: ["Metric", "Value", "Pct"] },
            { id: "ctx-positioning", key: "positioning_stretch", title: "Positioning", headers: ["Asset", "RSI", "vs 200DMA", "Stretch"] }
        ];

        sections.forEach((sec, idx) => {
            const data = mc[sec.key] || {};
            const active = idx === 0 ? "active" : "";
            let html = `<div class="tab-pane ${active}" id="${sec.id}">`;
            html += `<table class="dense-table data-table"><thead><tr>`;
            sec.headers.forEach(h => html += `<th>${h}</th>`);
            html += `</tr></thead><tbody>`;

            if(Object.keys(data).length === 0) {
                html += `<tr><td colspan="${sec.headers.length}">No data available</td></tr>`;
            } else {
                if(sec.key === "macro_rates") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object') continue;
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.latest_yield || v.latest_value)}</td>
                            <td class="num-cell">${formatNum(v.change_5d || v.move_5d_bps)}</td>
                            <td class="num-cell">${formatNum(v.change_20d || v.move_20d_bps)}</td>
                            <td class="num-cell">${formatNum(v.z_score_1y)}</td>
                            <td>${v.state_label || v.shape_label || '--'}</td>
                        </tr>`;
                    }
                } else if(sec.key === "credit_liquidity") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object') continue;
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.latest_oas || v.latest_price || v.latest_ratio)}</td>
                            <td class="num-cell">${formatNum(v.z_score_1y || v.z_score)}</td>
                            <td>${v.state_label || '--'}</td>
                            <td>${v.stretch_label || '--'}</td>
                        </tr>`;
                    }
                } else if(sec.key === "equity_index_state") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object') continue;
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum((v.return_5d||0)*100)}%</td>
                            <td class="num-cell">${formatNum((v.return_20d||0)*100)}%</td>
                            <td class="num-cell">${formatNum((v.dist_to_200dma||0)*100)}%</td>
                            <td class="num-cell">${formatNum(v.z_score_1y)}</td>
                            <td>${v.trend_state || '--'}</td>
                            <td>${v.stretch_state || '--'}</td>
                        </tr>`;
                    }
                } else if(sec.key === "sector_state") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object') continue;
                        const rel = v.relative_to_spy || {};
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum((rel.return_5d||0)*100)}%</td>
                            <td class="num-cell">${formatNum((rel.return_20d||0)*100)}%</td>
                            <td class="num-cell">${formatNum(rel.z_score_1y)}</td>
                            <td>${v.leadership_flag || '--'}</td>
                        </tr>`;
                    }
                } else if(sec.key === "volatility_stress") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object' && k.includes("flag")) {
                            html += `<tr><td>${k}</td><td colspan="3">${v}</td></tr>`;
                            continue;
                        } else if(typeof v !== 'object') continue;
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.realized_vol_20d || v.latest_value)}</td>
                            <td class="num-cell">${formatNum(v.vol_percentile_1y)}</td>
                            <td>${v.stress_state || v.state_label || '--'}</td>
                        </tr>`;
                    }
                } else if(sec.key === "flight_to_safety") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object') continue;
                        html += `<tr>
                            <td>${k}</td>
                            <td>${v.state_label || v.leadership_flag || '--'}</td>
                            <td>${v.stretch_label || '--'}</td>
                            <td>${v.description || '--'}</td>
                        </tr>`;
                    }
                } else if(sec.key === "cross_asset_relationships") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object') continue;
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.latest_ratio)}</td>
                            <td class="num-cell">${formatNum((v.return_5d||0)*100)}%</td>
                            <td class="num-cell">${formatNum(v.z_score_1y)}</td>
                            <td>${v.state_label || '--'}</td>
                        </tr>`;
                    }
                } else if(sec.key === "breadth_participation") {
                    for(const [k,v] of Object.entries(data)) {
                        let val = v; let pct = "--";
                        if(typeof v === 'object') {
                            val = v.count;
                            if(v.percentage !== undefined) pct = formatNum(v.percentage*100) + "%";
                        }
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${val}</td>
                            <td class="num-cell">${pct}</td>
                        </tr>`;
                    }
                } else if(sec.key === "positioning_stretch") {
                    for(const [k,v] of Object.entries(data)) {
                        if(typeof v !== 'object') continue;
                        html += `<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.rsi_14d)}</td>
                            <td class="num-cell">${formatNum((v.dist_to_200dma||0)*100)}%</td>
                            <td>${v.stretch_state || v.stretch_label || '--'}</td>
                        </tr>`;
                    }
                }
            }
            html += `</tbody></table></div>`;
            ctc.innerHTML += html;
        });
    }

    function renderAudit(data) {
        document.getElementById("audit-json").textContent = JSON.stringify(data, null, 2);
    }

    fetchData();
    setInterval(fetchData, 60000); // refresh every minute

});
