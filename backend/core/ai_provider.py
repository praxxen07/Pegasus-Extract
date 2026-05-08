import os
import logging
import time

from dotenv import load_dotenv


load_dotenv()

log = logging.getLogger("PegasusExtract")

# Cache of failed providers: {provider_key: failure_timestamp}
# Skip providers that failed in the last 5 minutes
_PROVIDER_FAIL_CACHE: dict[str, float] = {}
_FAIL_COOLDOWN = 300  # 5 minutes


def _is_provider_dead(key: str) -> bool:
    ts = _PROVIDER_FAIL_CACHE.get(key)
    if ts and (time.time() - ts) < _FAIL_COOLDOWN:
        return True
    return False


def _mark_provider_dead(key: str) -> None:
    _PROVIDER_FAIL_CACHE[key] = time.time()


class AIProvider:
    def __init__(self):
        self.gemini_key = os.getenv("GEMINI_API_KEY", "")
        self.deepseek_key = os.getenv("DEEPSEEK_API_KEY", "")
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.groq_key = os.getenv("GROQ_API_KEY", "")
        self.openrouter_key = os.getenv("OPENROUTER_API_KEY", "")

    # Free models to try on OpenRouter, in priority order.
    OPENROUTER_MODELS = [
        "nvidia/nemotron-3-super-120b-a12b:free",
        "meta-llama/llama-3.3-70b-instruct:free",
        "qwen/qwen3-coder:free",
        "qwen/qwen3-next-80b-a3b-instruct:free",
        "openai/gpt-oss-120b:free",
    ]

    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        json_mode: bool = False,
    ) -> dict:
        # ── 1. Try Gemini 2.0 Flash first (1M tokens/day free) ──
        if self.gemini_key and not _is_provider_dead("gemini"):
            try:
                from openai import OpenAI

                client = OpenAI(
                    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
                    api_key=self.gemini_key,
                )

                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
                kwargs: dict = {
                    "model": "gemini-2.0-flash",
                    "messages": messages,
                    "max_tokens": 4096,
                    "temperature": 0.1,
                }
                if json_mode:
                    kwargs["response_format"] = {"type": "json_object"}

                response = client.chat.completions.create(**kwargs)
                text = response.choices[0].message.content
                log.info("Gemini 2.0 Flash answered successfully")
                return {"text": text, "provider": "gemini"}
            except Exception as e:  # noqa: BLE001
                log.warning(f"Gemini failed: {e}")
                if "429" in str(e) or "quota" in str(e).lower():
                    _mark_provider_dead("gemini")

        # ── 2. Try Groq ──
        if self.groq_key and not _is_provider_dead("groq"):
            try:
                from groq import Groq

                if len(user_prompt) > 20000:
                    user_prompt = user_prompt[:20000] + "\n...[truncated]..."

                client = Groq(api_key=self.groq_key)

                def _build_kwargs(current_user_prompt: str) -> dict:
                    messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": current_user_prompt},
                    ]
                    kw: dict = {
                        "model": "llama-3.3-70b-versatile",
                        "messages": messages,
                        "max_tokens": 4096,
                        "temperature": 0.1,
                    }
                    if json_mode:
                        kw["response_format"] = {"type": "json_object"}
                    return kw

                try:
                    kwargs = _build_kwargs(user_prompt)
                    response = client.chat.completions.create(**kwargs)
                    text = response.choices[0].message.content
                    log.info("Groq answered successfully")
                    return {"text": text, "provider": "groq"}
                except Exception as e:  # noqa: BLE001
                    msg = str(e)
                    if "413" in msg or "too large" in msg.lower():
                        shorter = user_prompt[: len(user_prompt) // 2]
                        kwargs = _build_kwargs(shorter)
                        response = client.chat.completions.create(**kwargs)
                        text = response.choices[0].message.content
                        log.info("Groq answered successfully after truncation retry")
                        return {"text": text, "provider": "groq"}
                    log.error(f"Groq failed: {e}")
                    if "429" in str(e) or "rate_limit" in str(e).lower():
                        _mark_provider_dead("groq")
            except Exception as e:  # noqa: BLE001
                log.error(f"Groq also failed: {e}")

        # ── 3. DeepSeek direct API ──
        if self.deepseek_key and not _is_provider_dead("deepseek"):
            try:
                from openai import OpenAI

                client = OpenAI(
                    base_url="https://api.deepseek.com",
                    api_key=self.deepseek_key,
                )

                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
                kwargs: dict = {
                    "model": "deepseek-chat",
                    "messages": messages,
                    "max_tokens": 4096,
                    "temperature": 0.1,
                }
                if json_mode:
                    kwargs["response_format"] = {"type": "json_object"}

                response = client.chat.completions.create(**kwargs)
                text = response.choices[0].message.content
                log.info("DeepSeek answered successfully")
                return {"text": text, "provider": "deepseek"}
            except Exception as e:  # noqa: BLE001
                log.warning(f"DeepSeek failed: {e} — trying OpenRouter")
                if "402" in str(e) or "balance" in str(e).lower():
                    _mark_provider_dead("deepseek")

        # ── 4. OpenRouter (multiple free models) ──
        if self.openrouter_key:
            try:
                from openai import OpenAI

                client = OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=self.openrouter_key,
                )

                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]

                for model_id in self.OPENROUTER_MODELS:
                    if _is_provider_dead(f"openrouter/{model_id}"):
                        continue
                    try:
                        kwargs: dict = {
                            "model": model_id,
                            "messages": messages,
                            "max_tokens": 4096,
                            "temperature": 0.1,
                        }
                        if json_mode:
                            kwargs["response_format"] = {"type": "json_object"}

                        response = client.chat.completions.create(**kwargs)
                        text = response.choices[0].message.content
                        log.info(f"OpenRouter ({model_id}) answered successfully")
                        return {"text": text, "provider": f"openrouter/{model_id}"}
                    except Exception as model_err:  # noqa: BLE001
                        log.warning(f"OpenRouter {model_id} failed: {model_err}")
                        if "429" in str(model_err) or "402" in str(model_err) or "404" in str(model_err):
                            _mark_provider_dead(f"openrouter/{model_id}")
                        continue

                log.warning("All OpenRouter models failed — trying Claude")
            except Exception as e:  # noqa: BLE001
                log.warning(f"OpenRouter setup failed: {e} — trying Claude")

        # ── 5. Last resort: Claude direct ──
        if self.anthropic_key:
            try:
                import anthropic

                client = anthropic.Anthropic(api_key=self.anthropic_key)
                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=4096,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                text = response.content[0].text
                log.info("Claude answered successfully")
                return {"text": text, "provider": "claude"}
            except Exception as e:  # noqa: BLE001
                log.warning(f"Claude failed: {e}")

        return {
            "text": "{}",
            "provider": "none",
        }

    def status(self):
        return {
            "gemini": bool(self.gemini_key),
            "deepseek": bool(self.deepseek_key),
            "openrouter": bool(self.openrouter_key),
            "groq": bool(self.groq_key),
            "claude": bool(self.anthropic_key),
        }


ai_provider = AIProvider()

