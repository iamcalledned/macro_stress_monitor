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

    // Toggle Audit
    const auditToggle = document.getElementById("audit-toggle");
    const auditContent = document.getElementById("audit-content");
    if(auditToggle && auditContent) {
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
