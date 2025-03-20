from openai import OpenAI

api_key = "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"

client = OpenAI(api_key=api_key)

# List models
models = client.models.list()

print("Available models:")
for model in models.data:
    print(model.id)
