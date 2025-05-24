[![Run every day](https://github.com/franklinbaldo/caixaaberta/actions/workflows/actions.yaml/badge.svg)](https://github.com/franklinbaldo/caixaaberta/actions/workflows/actions.yaml)

# What is Caixa Aberta?
Caixa Aberta is a script to scrap em compile realstate data from the Caixa Econômica Federal (CEF) official site.

# Why?
The CEF site is a great source of information, but it is not always easy to find the information you need.
Also, there is no history of the data, so it is hard follow what happened in the past.

# For who?
This script is for people who want to know what is happening in their city, state and country.
Scholars, students, real estate agents, financiers, etc.

# Setup
To set up the project environment, you can use UV. If you don't have UV installed, you can install it with pip:
```bash
pip install uv
```
Alternatively, refer to the [official UV installation guide](https://github.com/astral-sh/uv#installation).

Once UV is installed, follow these steps:

```bash
# Create a virtual environment (optional but recommended)
# Using Python's built-in venv module:
python -m venv .venv
source .venv/bin/activate  # On Windows use .venv\Scripts\activate

# Or using UV:
uv venv
source .venv/bin/activate # On Windows use .venv\Scripts\activate

# Install dependencies
uv pip install -r requirements.txt

# Install development dependencies (optional)
uv pip install -r requirements-dev.txt
```

# TODO
- [ ] Better way to show the data
- [ ] Website
- [ ] Geodecoding
