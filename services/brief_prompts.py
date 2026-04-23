"""
Prompt Builder Layer.
Constructs strict prompts from structured deterministic outputs.
"""
import json
from typing import Dict, Any, Tuple

def get_system_prompt() -> str:
    return """You are generating internal market briefs based on structured system outputs.

Your job is to explain what the system is seeing in clear, practical English for a human analyst.

Rules:
- Do NOT restate obvious fields already visible (e.g. 'structural is calm')
- Start with the most important signal, not background context
- Rank signals: dominant first, supporting second
- Explain WHY each signal matters in plain English
- Avoid generic finance language ('markets are digesting', 'sentiment is mixed')
- Avoid weak phrasing ('suggesting', 'may indicate', 'could be')
- Be direct and specific
- Do NOT provide trade recommendations
- Do NOT invent data
- Always stay grounded in the provided facts

Morning Brief:
- Focus on the setup and key risks entering the day

Evening Wrap:
- Focus on what actually confirmed or failed during the day

Always:
- Highlight structural vs preview divergence clearly
- Explain consequences, not just observations
- Keep output concise and structured
- YOU MUST output pure JSON matching the requested schema. No markdown wrapping or conversational text outside the JSON object.

OUTPUT JSON SCHEMA:
{
  "headline": "A sharp, one-sentence summary of the current regime and main risk/support.",
  "sections": {
    "overall": "A brief paragraph explaining the primary structural state.",
    "what_matters": ["Bullet point 1", "Bullet point 2"],
    "risks": ["Risk point 1", "Risk point 2"],
    "supports": ["Support point 1", "Support point 2"],
    "structural_vs_preview": "A short explanation of whether the intraday preview confirms or diverges from the structural trend.",
    "watch_next": ["Item 1 to watch", "Item 2 to watch"],
    "caveats": ["Caveat 1", "Caveat 2"]
  }
}
"""

def build_morning_brief_prompt(brief_dict: Dict[str, Any], health: Dict[str, Any], comparison: Dict[str, Any]) -> Tuple[str, str]:
    """Builds the payload for the Morning Brief."""
    system = get_system_prompt()
    
    context = {
        "current_structural_state": brief_dict.get("summary_state", "UNKNOWN"),
        "dominant_factors": brief_dict.get("dominant_factors", []),
        "top_risks": brief_dict.get("top_risks", []),
        "top_supports": brief_dict.get("top_supports", []),
        "watch_items": brief_dict.get("watch_items", []),
        "caveats": brief_dict.get("caveats", []),
        "alignment_comparison": comparison,
        "anomalies": brief_dict.get("anomalies", []),
        "data_quality": health,
    }
    
    user = f"""Generate the MORNING BRIEF.
Start from dominant_factors. Everything else is supporting context.

The morning brief should answer:
- What environment am I walking into?
- What matters most today?
- What are the biggest risks and supports?
- Is the latest preview aligning with the structural foundation?
- What should I watch next?
- What caveats matter right now?

Provided Market Context:
{json.dumps(context, indent=2)}
"""
    return system, user

def build_evening_wrap_prompt(brief_dict: Dict[str, Any], health: Dict[str, Any], comparison: Dict[str, Any]) -> Tuple[str, str]:
    """Builds the payload for the Evening Wrap."""
    system = get_system_prompt()
    
    context = {
        "closing_structural_state": brief_dict.get("summary_state", "UNKNOWN"),
        "dominant_factors": brief_dict.get("dominant_factors", []),
        "top_risks": brief_dict.get("top_risks", []),
        "top_supports": brief_dict.get("top_supports", []),
        "watch_items": brief_dict.get("watch_items", []),
        "caveats": brief_dict.get("caveats", []),
        "alignment_comparison": comparison,
        "anomalies": brief_dict.get("anomalies", []),
        "data_quality": health,
    }
    
    user = f"""Generate the EVENING WRAP.
Start from dominant_factors. Everything else is supporting context.

The evening wrap should answer:
- What actually happened in the system state today?
- What confirmed and what failed to confirm?
- Did the day strengthen or weaken the structural picture?
- What matters next?
- What caveats remain?

Provided Market Context:
{json.dumps(context, indent=2)}
"""
    return system, user
