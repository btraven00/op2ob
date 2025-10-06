# op2ob

Requires `uv` and `aria2`.

Extraction step additionally requires `deno`, but you can skip it and use the pre-extracted json files.

## extract data

```
make extract TARGET=denoising
make extract-all
```

## datasets

```
# as a table
uv run datasets.py list denoising
# as json
uv run datasets.py list denoising --json
uv run datasets.py list denoising DATASET
uv run datasets.py fetch denoising DATASET
uv run datasets.py fetch denoising DATASET FILE
```
