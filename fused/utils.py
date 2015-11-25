from pathlib import Path
from contextlib import ExitStack


SCRIPTS = {}

# Optimizable in Python 3.5
with ExitStack() as es:
    for p in Path('scripts').glob('*.lua'):
        file = es.enter_context(p.open())
        SCRIPTS[p.stem] = file.read()