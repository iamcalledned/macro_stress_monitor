document.addEventListener("DOMContentLoaded", () => {

    async function fetchData() {
        try {
            const res = await fetch("/api/render/terminal");
            if(res.ok) {
                const payload = await res.json();
                renderTerminal(payload);
            } else {
                console.error("Failed to fetch render payload");
            }
        } catch (err) {
            console.error("Fetch error", err);
        }
    }

    function renderTerminal(data) {
        // Status Strip
        const ss = data.status_strip;
        if(ss && ss.structural) {
            document.getElementById("ts-regime").textContent = ss.structural.regime_text;
            document.getElementById("ts-regime").className = ss.structural.regime_class;
            document.getElementById("ts-score").textContent = ss.structural.score_text;
            document.getElementById("ts-score").className = ss.structural.score_class;
            document.getElementById("ts-struc-time").textContent = ss.structural.time_et;
            document.getElementById("ts-quality").textContent = ss.structural.quality_text;
            document.getElementById("ts-conf").textContent = ss.structural.confidence_text;
            document.getElementById("ts-flags").textContent = ss.structural.flags_text;
            document.getElementById("ts-flags").className = ss.structural.flags_class;
        }
        if(ss && ss.preview) {
            const badge = document.getElementById("ts-preview");
            badge.textContent = ss.preview.text;
            badge.className = ss.preview.class_name;
            document.getElementById("ts-prev-time").textContent = ss.preview.time_et;
        }

        // Structural Summary
        const st = data.structural_summary;
        if(st) {
            document.getElementById("ss-score").textContent = st.score_text;
            document.getElementById("ss-score").className = st.score_class;
            document.getElementById("ss-regime").textContent = st.regime_text;
            document.getElementById("ss-regime").className = st.regime_class;
            document.getElementById("ss-drivers").textContent = st.drivers_text;
            document.getElementById("ss-delta").textContent = st.delta_text;
            document.getElementById("ss-delta").className = st.delta_class;
        }

        // Preview Summary
        const pv = data.preview_summary;
        if(pv) {
            document.getElementById("ps-assessment").textContent = pv.assessment_text;
            document.getElementById("ps-session").textContent = pv.session_text;
            document.getElementById("ps-delta").textContent = pv.delta_text;
            let psHtml = "";
            (pv.components || []).forEach(c => {
                psHtml += `<tr><td>${c.label}</td><td>${c.value}</td></tr>`;
            });
            document.getElementById("ps-components").innerHTML = psHtml || "<tr><td colspan='2'>No components</td></tr>";
        }

        // Health & Mini Panels
        const hl = data.health_summary;
        if(hl) {
            document.getElementById("hl-struc-age").textContent = hl.structural_age_text;
            document.getElementById("hl-struc-age").className = hl.structural_age_class;
            document.getElementById("hl-prev-age").textContent = hl.preview_age_text;
            document.getElementById("hl-prev-age").className = hl.preview_age_class;
            document.getElementById("hl-exec").textContent = hl.execution_text;
            document.getElementById("hl-missing-crit").textContent = hl.critical_text;
            document.getElementById("hl-missing-crit").className = hl.critical_class;
            document.getElementById("hl-missing-noncrit").textContent = hl.noncritical_text;
            
            document.getElementById("mini-conf").textContent = hl.mini_conf_text;
            document.getElementById("mini-conf").className = hl.mini_conf_class;
            document.getElementById("mini-qual").textContent = hl.mini_qual_text;
            document.getElementById("mini-qual").className = hl.mini_qual_class;
            document.getElementById("mini-flags").innerHTML = hl.mini_flags_html;
        }

        // Notable Changes
        const ch = data.notable_changes;
        if(ch) {
            let html = "";
            (ch.rows || []).forEach(r => {
                html += `<li class="${r.class_name}" style="${r.style}">${r.text_html}</li>`;
            });
            document.getElementById("delta-list").innerHTML = html;
        }

        // Market Context Tabs
        const mc = data.market_context || [];
        const tabsContainer = document.getElementById("context-tabs");
        const contentContainer = document.getElementById("context-content");
        
        if(mc.length > 0) {
            let tabsHtml = "";
            let panelsHtml = "";
            
            mc.forEach((sec) => {
                const active = sec.is_active ? "active" : "";
                tabsHtml += `<button class="tab-btn ${active}" data-target="${sec.id}">${sec.tab_html}</button>`;
                
                let pHtml = `<div class="tab-pane ${active}" id="${sec.id}">`;
                pHtml += `<table class="dense-table data-table"><thead><tr>`;
                sec.headers.forEach(h => pHtml += `<th>${h}</th>`);
                pHtml += `</tr></thead><tbody>`;
                
                if(sec.rows && sec.rows.length > 0) {
                    sec.rows.forEach(r => {
                        pHtml += `<tr>`;
                        r.cells.forEach(c => {
                            pHtml += `<td class="${c.class || ''}">${c.html}</td>`;
                        });
                        pHtml += `</tr>`;
                    });
                } else {
                    pHtml += `<tr><td colspan="${sec.headers.length}">No data available</td></tr>`;
                }
                pHtml += `</tbody></table></div>`;
                panelsHtml += pHtml;
            });
            
            tabsContainer.innerHTML = tabsHtml;
            contentContainer.innerHTML = panelsHtml;
            
            // Re-bind tab events
            const TABS_NEW = document.querySelectorAll(".tab-btn");
            TABS_NEW.forEach(btn => {
                btn.addEventListener("click", () => {
                    TABS_NEW.forEach(t => t.classList.remove("active"));
                    btn.classList.add("active");
                    document.querySelectorAll(".tab-pane").forEach(p => p.classList.remove("active"));
                    const target = btn.getAttribute("data-target");
                    const pane = document.getElementById(target);
                    if(pane) pane.classList.add("active");
                });
            });
        }
        
        // Audit
        if(data.audit) {
            document.getElementById("audit-json").textContent = JSON.stringify(data.audit, null, 2);
        }
    }

    // Toggle Audit
    const auditToggle = document.getElementById("audit-toggle");
    const auditContent = document.getElementById("audit-content");
    if(auditToggle) {
        auditToggle.addEventListener("click", () => {
            auditContent.classList.toggle("hidden");
            auditToggle.textContent = auditContent.classList.contains("hidden") 
                ? "RAW AUDIT DRILLDOWN [+]" 
                : "RAW AUDIT DRILLDOWN [-]";
        });
    }

    fetchData();
    setInterval(fetchData, 60000); // refresh every minute

});
