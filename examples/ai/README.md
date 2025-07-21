# Fusionbase AI Examples

This directory contains examples demonstrating the Fusionbase AI agents and tools.

## Examples Overview

### Core Examples

1. **`company_research_agent.py`** - Command-line interface for company research
   - Full-featured CLI with all configuration options
   - Supports different output formats
   - Proxy configuration support

2. **`usage_example.py`** - Basic programmatic usage
   - Simple Python API usage
   - Shows how to integrate the agent in your code

### Tool Configuration Examples

3. **`custom_tools_configuration.py`** - Configure which tools the agent uses
   - Create minimal agents for speed
   - Financial-focused agents
   - Web-enhanced agents
   - Learn to optimize for specific use cases

4. **`using_tool_presets.py`** - Use pre-configured tool sets
   - Minimal, Financial, News, and Comprehensive presets
   - Quick setup without manual tool selection
   - Shows how to customize presets

### Structured Output Examples

5. **`structured_output_personnel.py`** - Extract data into structured schemas
   - Uses Pydantic models for type-safe output
   - Extracts company personnel information
   - Ready for database storage or APIs

6. **`output_formats_comparison.py`** - Compare output formats
   - See the difference between markdown and structured output
   - Understand when to use each format

7. **`system_prompt_example.py`** - Using system prompts
   - Pass system instructions via messages array
   - Guide agent behavior and tool usage
   - Enable intent-based token efficiency

## Quick Start

### Basic Research
```bash
python examples/ai/company_research_agent.py --query "Research SAP SE"
```

### With Custom Tools
```python
from fusionbase.ai.agents import create_company_research_agent, ToolPresets

# Use minimal tools for speed
agent = create_company_research_agent(
    ...,
    researcher_tools=ToolPresets.minimal_researcher_tools()
)
```

### With Structured Output
```python
from pydantic import BaseModel

class CompanyInfo(BaseModel):
    name: str
    revenue: float
    employees: int

agent = create_company_research_agent(
    ...,
    output_schema=CompanyInfo  # Get structured data
)
```

### With System Prompts
```python
# Pass system prompts in messages
result = await agent.ainvoke({
    "messages": [
        {
            "role": "system",
            "content": "Focus on ESG and sustainability aspects"
        },
        {
            "role": "user",
            "content": "Research Tesla Inc"
        }
    ]
})
```

## Requirements

Set these environment variables:
- `FUSIONBASE_API_KEY` - Your Fusionbase API key (required)
- `OPENAI_API_KEY` - Your OpenAI API key (required)
- `SERP_API_KEY` - For web search capabilities (optional)

## Features Demonstrated

### Custom Tools Configuration
- Control which data sources the agent can access
- Optimize for speed vs comprehensiveness
- Create specialized agents for specific tasks

### Structured Output
- Get typed, validated data instead of markdown
- Use Pydantic models or JSON schemas
- Perfect for automation and integrations

### Tool Presets
- Pre-configured tool sets for common scenarios
- Minimal, Financial, News, Comprehensive options
- Easy to extend and customize

### Proxy Support
- Simple proxy configuration
- Domain-specific proxy routing
- SSL verification options

## Running the Examples

Each example can be run directly:
```bash
python examples/ai/<example_name>.py
```

Most examples include helpful command-line arguments:
```bash
python examples/ai/company_research_agent.py --help
```

## Tips

1. **Start Simple**: Use `company_research_agent.py` for basic research
2. **Optimize Tools**: Use `custom_tools_configuration.py` to learn about tool selection
3. **Structure Data**: Use `structured_output_personnel.py` for API/database integration
4. **Use Presets**: Start with `using_tool_presets.py` for quick configuration

## Advanced Usage

### Custom Output Schema
Define your own Pydantic models for exactly the data you need.

### Tool Selection
Choose only the tools you need for faster, more focused research.

### Proxy Configuration
Route different domains through different proxies for optimal access.

See individual example files for detailed documentation and usage.
