# Contributing to Frontier Bench

Thank you for your interest in contributing to Frontier Bench! This document provides guidelines for contributing to the project.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Branching Strategy](#branching-strategy)
- [Commit Messages](#commit-messages)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment. Harassment and discrimination of any kind will not be tolerated.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/<your-username>/-Frontier-Bench-.git
   ```
3. Set up the development environment:
   ```bash
   cd -Frontier-Bench-
   git remote add upstream https://github.com/isackkibet/-Frontier-Bench-.git
   git pull upstream main
   ```

## Development Workflow

### Branching Strategy

We use a standard Git Flow-inspired branching model:

- `main` — production-ready code
- `feature/*` — new features
- `bugfix/*` — bug fixes
- `docs/*` — documentation updates
- `refactor/*` — code refactoring
- `chore/*` — maintenance tasks

Always create a new branch for your work:
```bash
git checkout -b feature/your-feature-name main
```

## Commit Messages

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>[optional scope]: <description>

[optional body]

[optional footer]
```

Types:
- `feat` — new feature
- `fix` — bug fix
- `docs` — documentation changes
- `style` — formatting, missing semicolons, etc.
- `refactor` — code refactoring
- `test` — adding/updating tests
- `chore` — maintenance tasks
- `ci` — CI configuration changes

Examples:
```bash
git commit -m "feat: add lease-based job claiming mechanism"
git commit -m "fix: resolve deadlock in dependency resolution"
git commit -m "docs: update API documentation"
```

## Code Style

- **Python**: Follow [PEP 8](https://peps.python.org/pep-0008/)
- Use `ruff` for linting:
  ```bash
  ruff check .
  ```
- Use type hints for all public functions
- Write docstrings for all public classes and functions
- Keep functions small and focused (single responsibility)

## Testing

Before submitting your contribution, ensure:

1. All tests pass:
   ```bash
   python -m pytest tests/ -v
   ```
2. Your code passes linting:
   ```bash
   ruff check .
   ```
3. The reference solution still scores 1:
   ```bash
   harbor run -p . -a oracle -e docker
   ```

## Pull Request Process

1. Create your branch from `main`
2. Make your changes and commit them
3. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```
4. Open a Pull Request against `main`
5. Fill in the PR template completely
6. Request a review from the maintainers

### PR Checklist
- [ ] Code follows the style guide
- [ ] All tests pass
- [ ] Lint passes
- [ ] Documentation updated if necessary
- [ ] Commit messages follow Conventional Commits

## Questions?

If you have questions, feel free to open an issue or reach out to the maintainers.

Thank you for contributing! Your contributions help make Frontier Bench better for everyone.
