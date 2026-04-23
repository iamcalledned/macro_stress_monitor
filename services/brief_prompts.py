"""
Prompt Builder Layer.
Constructs strict prompts from structured deterministic outputs.
"""

import json
from typing import Dict, Any, Tuple, List

try:
    from .signal_translation import build_human_meaning_packet
except ImportError:
    from services.signal_translation import build_human_meaning_packet


def get_system_prompt() -> str:
    return """You are generating internal market briefs from structured system outputs.

Your job is not to merely restate fields.
Your job is to produce a useful internal analyst note.

Rules:
- Use only the supplied facts
- Do NOT reference any asset, metric, ratio, or condition that is not explicitly present in the input
- Do NOT invent missing data
- Do NOT provide trade recommendations
- Do NOT restate obvious fields already visible in the UI unless needed for context
- Do NOT simply paraphrase the same point across multiple sections
- Start with the most important signal, not the background state
- Rank signals: dominant first, supporting second
- Distinguish clearly between:
  - what is happening
  - what is breaking
  - what is holding up
  - what is not yet confirmed
  - why it matters
  - what to watch next
- Explain why the dominant signals matter in plain English
- Use direct language
- Avoid generic finance filler
- Avoid weak phrasing such as:
  - 'suggesting potential'
  - 'may indicate'
  - 'could imply'
  - 'markets are digesting'
  - 'sentiment is mixed'
- If there are no meaningful supports, say so briefly and move on
- Mention stale, missing, or proxy-based caveats when relevant
- Be concise and structured
- Keep each section distinct and non-repetitive
- YOU MUST output pure JSON matching the requested schema. No markdown wrapping or conversational text outside the JSON object.

Morning Brief:
- Focus on the setup entering the day
- Emphasize what matters now, risks, supports, why it matters, alignment, and what to watch next

Evening Wrap:
- Focus on what confirmed, what failed to confirm, what changed, and what matters next
- Emphasize follow-through or non-confirmation

Always:
- Highlight structural vs preview divergence clearly
- Explain consequences, not just observations
- State whether stress looks localized, broadening, or unclear
- Sound like an internal desk note, not a retail newsletter

OUTPUT JSON SCHEMA:
{
  "headline": "A sharp, one-sentence summary of the current setup and main tension.",
  "sections": {
    "what_matters": ["Bullet point 1", "Bullet point 2"],
    "risks": ["Risk point 1", "Risk point 2"],
    "supports_or_confirmations": ["Support point 1", "Support point 2"],
    "why_this_matters": ["Explanation point 1", "Explanation point 2"],
    "watch_next": ["Item 1 to watch", "Item 2 to watch"],
    "alignment": "A short explanation of whether intraday preview confirms or diverges from the structural backdrop.",
    "caveats": ["Caveat 1", "Caveat 2"]
  }
}
"""


def build_morning_brief_prompt(
    brief_dict: Dict[str, Any],
    health: Dict[str, Any],
    comparison: Dict[str, Any],
) -> Tuple[str, str]:
    """Build the payload for the Morning Brief."""
    system = get_system_prompt()
    packet = build_human_meaning_packet(
        brief_dict=brief_dict,
        health=health,
        comparison=comparison,
        mode="morning_brief",
    )

    user = f"""Generate the MORNING BRIEF.

Start from dominant_signal and dominant_factors. Everything else is supporting context.

Write each section so it contributes something distinct:
- what_matters = dominant setup right now
- risks = what could worsen or spread
- supports_or_confirmations = what is holding up or confirming stability
- why_this_matters = practical consequence of the setup
- watch_next = next confirmation or invalidation points
- alignment = structural vs preview relationship
- caveats = confidence limits, stale data, missing data, or proxy limitations

Do NOT mention any asset, ratio, or metric not explicitly present in the packet.
Do NOT repeat the same sentence across sections.
If supports are weak or absent, say so briefly.

Structured Brief Packet:
{json.dumps(packet, indent=2)}
"""
    return system, user


def build_evening_wrap_prompt(
    brief_dict: Dict[str, Any],
    health: Dict[str, Any],
    comparison: Dict[str, Any],
) -> Tuple[str, str]:
    """Build the payload for the Evening Wrap."""
    system = get_system_prompt()
    packet = build_human_meaning_packet(
        brief_dict=brief_dict,
        health=health,
        comparison=comparison,
        mode="evening_wrap",
    )

    user = f"""Generate the EVENING WRAP.

Start from dominant_signal and what_changed. Everything else is supporting context.

Write each section so it contributes something distinct:
- what_matters = what defined the day by the close
- risks = what remains fragile or unresolved
- supports_or_confirmations = what actually held up or confirmed
- why_this_matters = what today's outcome means in practical terms
- watch_next = next confirmation or failure points
- alignment = whether preview and structural conditions ended up aligned or still diverging
- caveats = confidence limits, stale data, missing data, or proxy limitations

Emphasize:
- what confirmed
- what failed to confirm
- whether stress stayed localized or started broadening

Do NOT mention any asset, ratio, or metric not explicitly present in the packet.
Do NOT repeat the same sentence across sections.
If supports are weak or absent, say so briefly.

Structured Brief Packet:
{json.dumps(packet, indent=2)}
"""
    return system, user