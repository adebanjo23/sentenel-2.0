"""Unified LLM client — abstracts OpenAI and Anthropic behind a single async function."""

import logging

logger = logging.getLogger("sentinel.llm")


async def llm_chat(
    provider: str,
    model: str,
    system_message: str,
    user_message: str,
    api_key: str,
    temperature: float = 0.2,
    max_tokens: int = 4096,
) -> str:
    """
    Send a chat message to an LLM and return the raw text response.

    Args:
        provider: "openai" or "anthropic"
        model: Model identifier (e.g. "gpt-4.1", "claude-opus-4-6")
        system_message: System prompt
        user_message: User prompt
        api_key: API key for the provider
        temperature: Sampling temperature
        max_tokens: Maximum response tokens

    Returns:
        Raw text content from the LLM (markdown code blocks stripped)
    """
    if provider == "anthropic":
        raw = await _call_anthropic(model, system_message, user_message, api_key, temperature, max_tokens)
    else:
        raw = await _call_openai(model, system_message, user_message, api_key, temperature, max_tokens)

    # Strip markdown code blocks if present
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()

    return raw


async def _call_openai(
    model: str, system_message: str, user_message: str,
    api_key: str, temperature: float, max_tokens: int,
) -> str:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=api_key)
    logger.debug(f"OpenAI call: model={model}")

    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return response.choices[0].message.content or ""


async def _call_anthropic(
    model: str, system_message: str, user_message: str,
    api_key: str, temperature: float, max_tokens: int,
) -> str:
    from anthropic import AsyncAnthropic

    client = AsyncAnthropic(api_key=api_key)
    logger.debug(f"Anthropic call: model={model}")

    response = await client.messages.create(
        model=model,
        system=system_message,
        messages=[
            {"role": "user", "content": user_message},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return response.content[0].text if response.content else ""
