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
        // Apply generic DOM bindings
        if(data.bindings && Array.isArray(data.bindings)) {
            data.bindings.forEach(b => {
                const el = document.getElementById(b.id);
                if(el) {
                    if(b.text !== undefined) el.textContent = b.text;
                    if(b.class_name !== undefined) el.className = b.class_name;
                    if(b.html !== undefined) el.innerHTML = b.html;
                }
            });
        }
        
        // Re-bind tab events since market context tabs might have been re-rendered
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
        
        // Audit
        if(data.audit) {
            const auditEl = document.getElementById("audit-json");
            if(auditEl) {
                auditEl.textContent = JSON.stringify(data.audit, null, 2);
            }
        }
    }

    function setupLlmGenerators() {
        const btnMorning = document.getElementById("btn-gen-morning");
        const btnEvening = document.getElementById("btn-gen-evening");
        
        if (btnMorning) {
            btnMorning.addEventListener("click", async () => {
                btnMorning.textContent = "[WAIT...]";
                try {
                    const res = await fetch("/api/brief/morning/generate", {method: "POST"});
                    if (!res.ok) throw new Error("Failed to generate");
                    await fetchData(); // Reload UI
                } catch(err) {
                    alert("Error generating Morning Brief: " + err);
                } finally {
                    btnMorning.textContent = "[GENERATE]";
                }
            });
        }
        
        if (btnEvening) {
            btnEvening.addEventListener("click", async () => {
                btnEvening.textContent = "[WAIT...]";
                try {
                    const res = await fetch("/api/brief/evening/generate", {method: "POST"});
                    if (!res.ok) throw new Error("Failed to generate");
                    await fetchData(); // Reload UI
                } catch(err) {
                    alert("Error generating Evening Wrap: " + err);
                } finally {
                    btnEvening.textContent = "[GENERATE]";
                }
            });
        }
    }

    // Toggle audit panel
    const auditToggle = document.getElementById("audit-toggle");
    if(auditToggle) {
        auditToggle.addEventListener("click", () => {
            const content = document.getElementById("audit-content");
            if(content.classList.contains("hidden")) {
                content.classList.remove("hidden");
                auditToggle.textContent = "RAW AUDIT DRILLDOWN [-]";
            } else {
                content.classList.add("hidden");
                auditToggle.textContent = "RAW AUDIT DRILLDOWN [+]";
            }
        });
    }

    setupLlmGenerators();
    fetchData();
    setInterval(fetchData, 60000); // refresh every minute

});
