# carbon-footprint
ETL Pipeline for carbon footprint evolution by country since 2010 based on the data provided by the Global Footprint Network.

## Setup

This project uses [uv](https://docs.astral.sh/uv/) for Python package and environment management.

### Prerequisites

- Python 3.9 or higher
- uv (install via `pip install uv`)

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   uv sync
   ```

This will create a virtual environment in `.venv` and install the project in editable mode.

### Usage

Run Python scripts with uv:
```bash
uv run python your_script.py
```

Or activate the virtual environment:
```bash
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows
```
