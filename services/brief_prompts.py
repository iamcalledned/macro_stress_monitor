"""
Prompt Builder Layer.
Constructs strict prompts from structured deterministic outputs.
"""
import json
from typing import Dict, Any, Tuple

def get_system_prompt() -> str:
    return """You are generating internal market briefs based only on structured system outputs provided to you.

Your job is to explain what the system is seeing in clear, practical English for a human analyst.

Rules:
- Use only the supplied facts
- Do NOT reference any asset, metric, ratio, or condition that is not explicitly present in the input
- Do NOT invent missing data
- Do NOT provide trade recommendations
- Do NOT restate obvious fields already visible in the UI unless needed for context
- Start with the most important signal, not the background state
- Rank signals: dominant first, supporting second
- Explain why the dominant signals matter in plain English
- Use direct language
- Avoid generic finance filler
- Avoid weak phrasing such as:
  - 'suggesting potential'
  - 'may indicate'
  - 'could imply'
  - 'markets are digesting'
  - 'sentiment is mixed'
- Mention stale, missing, or proxy-based caveats when relevant
- Be concise and structured

Morning Brief:
- Focus on the setup entering the day
- Emphasize what matters now, top risks, top supports, alignment, and what to watch next

Evening Wrap:
- Focus on what confirmed, what failed to confirm, and what changed by the close
- Emphasize follow-through or non-confirmation

Always:
- Highlight structural vs preview divergence clearly
- Explain consequences, not just observations
- Sound like an internal desk note, not a retail newsletter
- YOU MUST output pure JSON matching the requested schema. No markdown wrapping or conversational text outside the JSON object.

OUTPUT JSON SCHEMA:
{
  "headline": "A sharp, one-sentence summary of the current regime and main risk/support.",
  "sections": {
    "what_matters": ["Bullet point 1", "Bullet point 2"],
    "risks": ["Risk point 1", "Risk point 2"],
    "supports_or_confirmations": ["Support point 1", "Support point 2"],
    "why_this_matters": ["Explanation point 1", "Explanation point 2"],
    "watch_next": ["Item 1 to watch", "Item 2 to watch"],
    "alignment": "A short explanation of whether the intraday preview confirms or diverges from the structural trend.",
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
