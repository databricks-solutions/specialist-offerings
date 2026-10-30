# CI for this project

The host repository has no GitHub Actions configured, so this project ships its workflow as an example.
To enable it, copy `github-workflow.yml.example` to `.github/workflows/onpremhdfs2managediceberg.yml` at the repository
root and add, under `on.push` / `on.pull_request`, `paths: [data-engineering/onpremhdfs2managediceberg/**]`, plus
`defaults: { run: { working-directory: data-engineering/onpremhdfs2managediceberg } }` to every job.

Until then the gate is local: `make check` (ruff, writer lint, unit tests, secret scan) and the project-level
pre-commit hooks (`pre-commit install -c data-engineering/onpremhdfs2managediceberg/.pre-commit-config.yaml`).
