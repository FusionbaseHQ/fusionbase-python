# Fusionbase AI Examples

This directory contains examples for using Fusionbase AI tools and agents.

## Company Research Agent

The company research agent is a powerful tool for gathering comprehensive information about companies using both Fusionbase data and web research.

### Basic Usage

```python
from fusionbase import Fusionbase
from fusionbase.ai.agents import create_company_research_agent
from langchain_openai import ChatOpenAI

# Initialize clients
fb_client = Fusionbase()
model = ChatOpenAI(model="gpt-4o")

# Create agent
agent = create_company_research_agent(
    fusionbase_client=fb_client,
    planner_model=model,
    researcher_model=model,
    synthesizer_model=model,
    hallucination_grader_model=model
)

# Run research
result = await agent.ainvoke({
    "messages": [{"role": "user", "content": "Research Fusionbase GmbH"}]
})
```

### Proxy Configuration

The agent supports both simple and domain-specific proxy configurations:

#### Simple Proxy Configuration

```python
simple_proxies = {
    "http": "http://proxy:8080",
    "https": "https://proxy:8080"
}

agent = create_company_research_agent(
    # ... other parameters ...
    proxies=simple_proxies,
    verify_ssl=False  # Disable SSL verification if needed
)
```

#### Domain-Specific Proxy Configuration

```python
domain_proxies = {
    "*": {  # Default proxy for all domains
        "http": "http://default-proxy:8080",
        "https": "https://default-proxy:8080"
    },
    "linkedin.com": {  # Specific proxy for LinkedIn
        "http": "http://linkedin-proxy:8080",
        "https": "https://linkedin-proxy:8080"
    },
    "*.linkedin.com": {  # Proxy for LinkedIn subdomains
        "http": "http://linkedin-proxy:8080",
        "https": "https://linkedin-proxy:8080"
    }
}

agent = create_company_research_agent(
    # ... other parameters ...
    proxies=domain_proxies
)
```

### Command Line Usage

```bash
# Simple proxy
python examples/ai/company_research_agent.py --query "Research MediaMir GmbH" --proxy "http://proxy:8080"

# Domain-specific proxy from file
python examples/ai/company_research_agent.py --query "Research MediaMir GmbH" --proxy-config proxy_config.json

# With SSL verification disabled
python examples/ai/company_research_agent.py --query "Research MediaMir GmbH" --proxy "http://proxy:8080" --no-verify-ssl
```

### Proxy Configuration File Format

Create a JSON file with domain-specific proxy settings:

```json
{
  "*": {
    "http": "http://default-proxy:8080",
    "https": "https://default-proxy:8080"
  },
  "linkedin.com": {
    "http": "http://linkedin-proxy:8080",
    "https": "https://linkedin-proxy:8080"
  },
  "*.linkedin.com": {
    "http": "http://linkedin-proxy:8080",
    "https": "https://linkedin-proxy:8080"
  }
}
```

### Environment Variables

- `FUSIONBASE_API_KEY`: Your Fusionbase API key
- `OPENAI_API_KEY`: Your OpenAI API key
- `SERP_API_KEY`: Your SERP API key for web search

## Files

- `company_research_agent.py`: Command-line interface for the research agent
- `usage_example.py`: Programmatic usage examples with different proxy configurations
- `proxy_config_example.json`: Example proxy configuration file
