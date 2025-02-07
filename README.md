# BOTO Playground

Prepare your .env file in the project directory
Rename env.sample to .env and fill in your environment variables

Use uv as environment and package manager

https://docs.astral.sh/uv/getting-started/installation/
```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Once uv is installed, restart your shell, then inside project directory run
```
uv sync
```

To read the help doc of the python script if available, run
```
uv run path-to-script.py --help
```

To run the python script, run
```
uv run path-to-script.py --parameters argument
```
