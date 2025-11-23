import google.generativeai as genai
import dspy

class FakeUsage:
    def __init__(self, prompt_tokens=0, completion_tokens=0):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = prompt_tokens + completion_tokens
    
    def __iter__(self):
        yield ('prompt_tokens', self.prompt_tokens)
        yield ('completion_tokens', self.completion_tokens)
        yield ('total_tokens', self.total_tokens)

class FakeMessage:
    def __init__(self, content):
        self.content = content

class FakeChoice:
    def __init__(self, content):
        self.message = FakeMessage(content)

class FakeResponse:
    def __init__(self, content, prompt_tokens=0, completion_tokens=0, model="gemini"):
        self.choices = [FakeChoice(content)]
        self.usage = FakeUsage(prompt_tokens, completion_tokens)
        self.model = model

class GeminiLM(dspy.LM):
    def __init__(self, model, api_key):
        super().__init__(model="custom_gemini")

        genai.configure(api_key=api_key)
        self.model_name = model
        self.client = genai.GenerativeModel(model_name=model)

    def forward(self, prompt=None, messages=None, **kwargs):
        # Build prompt text from DSPy's format
        contents = "\n".join(
            m["content"] for m in messages if m.get("role") == "user"
        ) if messages else prompt

        # Generate content via Gemini
        response = self.client.generate_content(contents)

        # Extract response text
        text = response.text if hasattr(response, "text") else str(response)

        # Fake usage
        prompt_tokens = len(contents) // 4
        completion_tokens = len(text) // 4

        return FakeResponse(
            content=text,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=self.model_name,
        )
