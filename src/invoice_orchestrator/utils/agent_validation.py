from src.invoice_orchestrator.classes.agent import Agent

import logging

# Configure logger
logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

agent = Agent(model="gpt-4o-mini",
              api_key="...",
              logger=logger)

prompt = "This prompt is a test trying to use OpenAI API. Give me feedback if it is working!"
response = agent.call_openai_api(prompt)
print(response)