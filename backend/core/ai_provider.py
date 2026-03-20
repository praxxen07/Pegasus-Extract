import os
import logging

from dotenv import load_dotenv


load_dotenv()

log = logging.getLogger("PegasusExtract")


class AIProvider:
    def __init__(self):
        self.deepseek_key = os.getenv("DEEPSEEK_API_KEY", "")
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.groq_key = os.getenv("GROQ_API_KEY", "")
        self.openrouter_key = os.getenv("OPENROUTER_API_KEY", "")

    # Free models to try on OpenRouter, in priority order.
    OPENROUTER_MODELS = [
        "meta-llama/llama-3.3-70b-instruct:free",
        "qwen/qwen3-coder:free",
        "openai/gpt-oss-120b:free",
        "qwen/qwen3-next-80b-a3b-instruct:free",
        "nvidia/nemotron-3-super-120b-a12b:free",
    ]

    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        json_mode: bool = False,
    ) -> dict:
        # ── 1. DeepSeek direct API (top priority — paid, reliable, great at code) ──
        if self.deepseek_key:
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

        # ── 2. OpenRouter (multiple free models) ──
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
                        continue

                log.warning("All OpenRouter models failed — trying Groq")
            except Exception as e:  # noqa: BLE001
                log.warning(f"OpenRouter setup failed: {e} — trying Groq")

        # ── 3. Fallback to Groq ──
        if self.groq_key:
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
            except Exception as e:  # noqa: BLE001
                log.error(f"Groq also failed: {e}")

        # ── 4. Last resort: Claude direct ──
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
            "deepseek": bool(self.deepseek_key),
            "openrouter": bool(self.openrouter_key),
            "groq": bool(self.groq_key),
            "claude": bool(self.anthropic_key),
        }


ai_provider = AIProvider()

