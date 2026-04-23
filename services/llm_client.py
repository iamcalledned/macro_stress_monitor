"""
Local vLLM Client.
A zero-dependency HTTP client for querying a local OpenAI-compatible API.
"""
import os
import json
import urllib.request
import urllib.error
from typing import Dict, Any

def generate_completion(system_prompt: str, user_prompt: str, temperature: float = 0.2, max_tokens: int = 1500) -> Dict[str, Any]:
    """Sends a completion request to the local vLLM instance."""
    
    enabled_str = str(os.getenv("MSM_LLM_ENABLED", "true")).lower()
    if enabled_str not in ("true", "1", "yes"):
        return {"success": False, "error": "LLM is disabled via MSM_LLM_ENABLED.", "meta": {}}
        
    base_url = os.getenv("MSM_LLM_BASE_URL", "http://127.0.0.1:30000/v1").rstrip("/")
    model = os.getenv("MSM_LLM_MODEL", "Qwen2.5-14B-Instruct-AWQ")
    timeout = int(os.getenv("MSM_LLM_TIMEOUT_SECONDS", "60"))
    
    endpoint = f"{base_url}/chat/completions"
    
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"}
    }
    
    req = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    
    meta = {"model": model, "base_url": base_url}
    
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
            content = result["choices"][0]["message"]["content"]
            
            try:
                # Ensure the LLM returned valid JSON as requested by response_format
                json_content = json.loads(content)
                return {
                    "success": True,
                    "text": content,
                    "parsed": json_content,
                    "meta": meta
                }
            except json.JSONDecodeError:
                return {
                    "success": False,
                    "error": "LLM returned invalid JSON format.",
                    "raw_text": content,
                    "meta": meta
                }
                
    except urllib.error.URLError as e:
        return {"success": False, "error": f"Connection failed: {e.reason}", "meta": meta}
    except Exception as e:
        return {"success": False, "error": str(e), "meta": meta}

def generate_structured_summary(system_prompt: str, user_prompt: str) -> Dict[str, Any]:
    """Generates a strict JSON summary."""
    return generate_completion(system_prompt, user_prompt, temperature=0.1)
