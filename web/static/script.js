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

    function classFromState(state) {
        if(!state) return "";
        const s = String(state).toLowerCase();
        if(s.includes("ok") || s.includes("calm") || s.includes("normal") || s.includes("stable") || s.includes("safe") || s.includes("positive") || s.includes("uptrend") || s.includes("leadership") || s.includes("outperforming") || s === "yes") return "val-positive";
        if(s.includes("elevated") || s.includes("watch") || s.includes("mixed") || s.includes("lag") || s.includes("range") || s.includes("flat")) return "val-warning";
        if(s.includes("breakdown") || s.includes("high") || s.includes("triggered") || s.includes("fast") || s.includes("risk") || s.includes("stress") || s.includes("underperforming") || s.includes("downtrend") || s.includes("overbought") || s.includes("oversold")) return "val-negative";
        if(s.includes("unavailable") || s.includes("insufficient") || s.includes("--")) return "color-muted";
        return "val-neutral";
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
                let rows = [];

                if(sec.key === "macro_rates") {
                    const addRow = (k, v) => {
                        if(!v) return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.latest_bps ?? v.latest)}</td>
                            <td class="num-cell">${formatNum(v.change_5d_bps ?? v.change_5d)}</td>
                            <td class="num-cell">${formatNum(v.change_20d_bps ?? v.change_20d)}</td>
                            <td class="num-cell">${formatNum(v.z_score_1y)}</td>
                            <td class="${classFromState(v.state)}">${v.state || '--'}</td>
                        </tr>`);
                    };
                    if(data.rates) Object.entries(data.rates).forEach(([k,v]) => addRow(k, v));
                    if(data.curve_spreads) Object.entries(data.curve_spreads).forEach(([k,v]) => addRow(k, v));
                    if(data.inflation_growth) Object.entries(data.inflation_growth).forEach(([k,v]) => addRow(k, v));
                    if(data.dollar_proxy?.state) addRow("Dollar (UUP)", data.dollar_proxy.state);
                    if(data.real_rate_proxy?.state) addRow("Real Rate (10Y-BE)", data.real_rate_proxy.state);
                } else if(sec.key === "credit_liquidity") {
                    const addRow = (k, v) => {
                        if(!v) return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.latest ?? v.latest_ratio ?? v.latest_bps)}</td>
                            <td class="num-cell">${formatNum(v.z_score_1y ?? v.z_score)}</td>
                            <td class="${classFromState(v.state)}">${v.state || '--'}</td>
                            <td class="${classFromState(v.stretch_state)}">${v.stretch_state || '--'}</td>
                        </tr>`);
                    };
                    if(data.ig_oas) addRow("IG OAS", data.ig_oas);
                    if(data.hy_oas) addRow("HY OAS", data.hy_oas);
                    if(data.loan_proxy) addRow("Loan Proxy (BKLN)", data.loan_proxy);
                    if(data.credit_etf_relationships) Object.entries(data.credit_etf_relationships).forEach(([k,v]) => addRow(k, v));
                    if(data.liquidity_sensitive_proxies) Object.entries(data.liquidity_sensitive_proxies).forEach(([k,v]) => addRow(k, v));
                } else if(sec.key === "equity_index_state") {
                    Object.entries(data).forEach(([k,v]) => {
                        if(!v || typeof v !== 'object') return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum((v.return_5d||0)*100)}%</td>
                            <td class="num-cell">${formatNum((v.return_20d||0)*100)}%</td>
                            <td class="num-cell">${formatNum((v.distance_200dma||0)*100)}%</td>
                            <td class="num-cell">${formatNum(v.z_score_1y)}</td>
                            <td class="${classFromState(v.trend_state)}">${v.trend_state || '--'}</td>
                            <td class="${classFromState(v.stretch_state)}">${v.stretch_state || '--'}</td>
                        </tr>`);
                    });
                } else if(sec.key === "sector_state") {
                    Object.entries(data).forEach(([k,v]) => {
                        if(!v || typeof v !== 'object') return;
                        const rel = v.relative_to_spy || {};
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum((rel.return_5d||0)*100)}%</td>
                            <td class="num-cell">${formatNum((rel.return_20d||0)*100)}%</td>
                            <td class="num-cell">${formatNum(rel.z_score_1y)}</td>
                            <td class="${classFromState(v.leadership_flag ? 'YES' : (v.laggard_flag ? 'LAG' : '--'))}">${v.leadership_flag ? "YES" : (v.laggard_flag ? "LAG" : "--")}</td>
                        </tr>`);
                    });
                } else if(sec.key === "volatility_stress") {
                    const addRow = (k, v) => {
                        if(!v) return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.realized_vol_20d)}</td>
                            <td class="num-cell">${formatNum(v.realized_vol_percentile_1y)}</td>
                            <td class="${classFromState(v.vol_state || v.trend_state)}">${v.vol_state || v.trend_state || '--'}</td>
                        </tr>`);
                    };
                    if(data.vix_proxy?.state) addRow("VIX Proxy (VIXY)", data.vix_proxy.state);
                    if(data.move_proxy?.state) addRow("MOVE Proxy (TLT Vol)", data.move_proxy.state);
                    if(data.realized_volatility) Object.entries(data.realized_volatility).forEach(([k,v]) => addRow(k, v));
                    if(data.stress_flags) {
                        Object.entries(data.stress_flags).forEach(([k,v]) => {
                            rows.push(`<tr><td>${k}</td><td class="num-cell">--</td><td class="num-cell">--</td><td class="${classFromState(v ? 'TRIGGERED' : 'OK')}">${v ? "TRIGGERED" : "OK"}</td></tr>`);
                        });
                    }
                } else if(sec.key === "flight_to_safety") {
                    const addRow = (k, v) => {
                        if(!v) return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="${classFromState(v.state || v.trend_state)}">${v.state || v.trend_state || '--'}</td>
                            <td class="${classFromState(v.stretch_state)}">${v.stretch_state || '--'}</td>
                            <td>--</td>
                        </tr>`);
                    };
                    if(data.treasury_proxies) Object.entries(data.treasury_proxies).forEach(([k,v]) => addRow(k, v));
                    if(data.gold_proxy) addRow("Gold Proxy (GLD)", data.gold_proxy);
                    if(data.dollar_proxy) addRow("Dollar Proxy (UUP)", data.dollar_proxy);
                    if(data.jpy_proxy) addRow("JPY Proxy", data.jpy_proxy);
                    if(data.defensive_vs_cyclical) Object.entries(data.defensive_vs_cyclical).forEach(([k,v]) => addRow(k, v));
                } else if(sec.key === "cross_asset_relationships") {
                    Object.entries(data).forEach(([k,v]) => {
                        if(!v || typeof v !== 'object') return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.latest_ratio)}</td>
                            <td class="num-cell">${formatNum((v.return_5d||0)*100)}%</td>
                            <td class="num-cell">${formatNum(v.z_score_1y)}</td>
                            <td class="${classFromState(v.state)}">${v.state || '--'}</td>
                        </tr>`);
                    });
                } else if(sec.key === "breadth_participation") {
                    const addRow = (k, v, pct) => {
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${v}</td>
                            <td class="num-cell">${pct}</td>
                        </tr>`);
                    };
                    if(data.tracked_count !== undefined) addRow("Tracked ETFs", data.tracked_count, "--");
                    if(data.above_50dma_count !== undefined) addRow("Above 50DMA", data.above_50dma_count, formatNum((data.above_50dma_pct||0)*100)+"%");
                    if(data.above_200dma_count !== undefined) addRow("Above 200DMA", data.above_200dma_count, formatNum((data.above_200dma_pct||0)*100)+"%");
                    if(data.positive_20d_trend_count !== undefined) addRow("Pos 20d Trend", data.positive_20d_trend_count, formatNum((data.positive_20d_trend_pct||0)*100)+"%");
                    if(data.sectors_above_50dma_count !== undefined) addRow("Sectors Above 50DMA", data.sectors_above_50dma_count, "--");
                    if(data.sectors_above_200dma_count !== undefined) addRow("Sectors Above 200DMA", data.sectors_above_200dma_count, "--");
                } else if(sec.key === "positioning_stretch") {
                    if(data.assets) Object.entries(data.assets).forEach(([k,v]) => {
                        if(!v) return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">${formatNum(v.rsi_14d)}</td>
                            <td class="num-cell">${formatNum((v.distance_200dma||0)*100)}%</td>
                            <td class="${classFromState(v.stretch_state)}">${v.stretch_state || '--'}</td>
                        </tr>`);
                    });
                    if(data.relationships) Object.entries(data.relationships).forEach(([k,v]) => {
                        if(!v) return;
                        rows.push(`<tr>
                            <td>${k}</td>
                            <td class="num-cell">--</td>
                            <td class="num-cell">${formatNum((v.distance_50dma||0)*100)}%</td>
                            <td class="${classFromState(v.stretch_state)}">${v.stretch_state || '--'}</td>
                        </tr>`);
                    });
                }

                if(rows.length === 0) {
                    html += `<tr><td colspan="${sec.headers.length}">No data available</td></tr>`;
                } else {
                    html += rows.join("");
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
