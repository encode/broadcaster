# Releasing

## Prerequisites

- Ensure release requirements are installed `pip install -r requirements-release.txt`

## Push to GitHub

Change from patch to minor or major for appropriate version updates in `broadcaster/__init__.py`, then push it to git.

```bash
<edit init>
git tag <init_version>
git push upstream && git push upstream --tags
```

## Push to PyPI

```bash
rm -rf dist/*
rm -rf build/*
python setup.py sdist bdist_wheel
twine upload dist/*
```
