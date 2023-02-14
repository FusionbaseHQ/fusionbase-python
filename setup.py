from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name                          = "fusionbase",
    version                       = "0.2.8",
    author                        = "Fusionbase",
    author_email                  = "info@fusionbase.com",
    description                   = "This is the main Fusionbase python package to make handling and interacting with data a piece of cake.",
    long_description              = long_description,
    long_description_content_type = 'text/markdown',
    url                           = "https://fusionbase.com",
    project_urls={
        "Issues": "https://github.com/FusionbaseHQ/fusionbase-python/issues",
        "GitHub": "https://github.com/FusionbaseHQ/fusionbase-python",
        "Docs": "https://developer.fusionbase.com/docs/developer-sdk/python"
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    packages                      = find_packages(),
    install_requires=[
          'requests-toolbelt',
          'tqdm',
          'rich',
          'pathos',
          'aiohttp',
          'aiofiles'
    ],
)