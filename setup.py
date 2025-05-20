from setuptools import find_packages
from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="fusionbase",
    version="1.0.0",
    description="Fusionbase Python SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Fusionbase",
    author_email="support@fusionbase.com",
    url="https://github.com/fusionbasehq/fusionbase-python",
    packages=find_packages(),
    install_requires=[
        "httpx>=0.23.0",
        "pydantic>=2.0.0",
        "tenacity>=8.0.0",
        "diskcache>=5.0.0",
        "loguru>=0.6.0",
        "psutil>=5.9.0",
    ],
    extras_require={
        "pandas": ["pandas>=1.5.0"],
        "msgpack": ["msgpack>=1.0.0"],
        "rich": ["rich>=10.0.0"],  # Add rich as an optional dependency
        "ai": [
            "langchain>=0.3,<0.4",
            "langchain-core>=0.3,<0.4",
            "langchain-openai>=0.2,<0.3",
            "langgraph>=0.0.16",  # Added for agent graph functionality
            "beautifulsoup4>=4.12.2",
            "html2text>=2020.1.16",  # Added for HTML to Markdown conversion
            "httpx>=0.23.0",
            "rich>=10.0.0",  # For formatted console output
        ],
        "all": [
            "pandas>=1.5.0",
            "msgpack>=1.0.0",
            "rich>=10.0.0",
            "langchain>=0.3,<0.4",
            "langchain-core>=0.3,<0.4",
            "langchain-openai>=0.2,<0.3",
            "langgraph>=0.0.16",  # Added for agent graph functionality
            "beautifulsoup4>=4.12.2",
            "html2text>=2020.1.16",  # Added for HTML to Markdown conversion
            "httpx>=0.23.0",
            "rich>=10.0.0",  # For formatted console output
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
