from pathlib import Path

from jinja2 import Environment, FileSystemLoader

# Create a Jinja2 environment with the templates directory as the loader path
env = Environment(loader=FileSystemLoader(Path(__file__).parent.joinpath("templates")))
