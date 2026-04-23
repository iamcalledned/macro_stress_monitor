"""
Prompt Builder Layer.
Constructs strict prompts from structured deterministic outputs.
"""
import json
from typing import Dict, Any, Tuple

def get_system_prompt() -> str:
    return """You are a disciplined quantitative market analyst. 
Your job is to read deterministic data foundation snapshots and summarize the current market state for a trading desk operator.

STRICT RULES:
1. DO NOT invent or hallucinate data. Use ONLY the provided JSON context.
2. DO NOT provide investment or trade advice.
3. Keep the tone grounded, slightly formal, and serious. Avoid generic finance fluff (e.g., "markets are digesting", "investors are weighing").
4. Explain what the data means practically, focusing on cross-asset alignment, deterioration, or strength.
5. Explicitly mention stale data or missing critical data caveats if they are present in the context.
6. YOU MUST output pure JSON matching the requested schema. No markdown wrapping or conversational text outside the JSON object.

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
        "top_risks": brief_dict.get("top_risks", []),
        "top_supports": brief_dict.get("top_supports", []),
        "watch_items": brief_dict.get("watch_items", []),
        "caveats": brief_dict.get("caveats", []),
        "alignment_comparison": comparison,
        "anomalies": brief_dict.get("anomalies", []),
        "dominant_factors": brief_dict.get("dominant_factors", []),
    }
    
    user = f"""Generate the MORNING BRIEF.
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
        "top_risks": brief_dict.get("top_risks", []),
        "top_supports": brief_dict.get("top_supports", []),
        "watch_items": brief_dict.get("watch_items", []),
        "caveats": brief_dict.get("caveats", []),
        "alignment_comparison": comparison,
        "anomalies": brief_dict.get("anomalies", []),
        "dominant_factors": brief_dict.get("dominant_factors", []),
    }
    
    user = f"""Generate the EVENING WRAP.
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
