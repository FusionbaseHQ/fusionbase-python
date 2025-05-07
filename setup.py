from setuptools import find_packages
from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="fusionbase-python",
    version="0.3.0",
    description="Fusionbase Python SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Fusionbase",
    author_email="support@fusionbase.com",
    url="https://github.com/fusionbase/fusionbase-python",
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
        "all": ["pandas>=1.5.0", "msgpack>=1.0.0", "rich>=10.0.0"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
