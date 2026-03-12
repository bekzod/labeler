# JSONL Labeler

A minimal web UI for labeling JSONL datasets. No dependencies beyond Python 3.10+.

## Setup

```bash
python app.py --jsonl path/to/data.jsonl
```

Open `http://127.0.0.1:8765` in your browser.

### Options

- `--host` — bind address (default: `127.0.0.1`)
- `--port` — port (default: `8765`)
