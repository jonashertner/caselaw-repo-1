from .base import ProviderBase as ProviderBase  # noqa: F401
from .base import ProviderMessage as ProviderMessage  # noqa: F401
from .base import ToolCall as ToolCall  # noqa: F401
from .base import ToolResult as ToolResult  # noqa: F401
from .openai_adapter import OpenAIProvider
from .gemini_adapter import GeminiProvider
from .anthropic_adapter import AnthropicProvider
from .ollama_adapter import OllamaProvider

PROVIDERS = {
    "openai": OpenAIProvider,
    "gemini": GeminiProvider,
    "claude": AnthropicProvider,
    "qwen2.5": lambda: OllamaProvider(model="qwen2.5:14b"),
    "llama3.3": lambda: OllamaProvider(model="llama3.3:70b"),
}
