# Contributing to BarAlgae Data Infrastructure

Thank you for your interest in contributing to the BarAlgae Data Infrastructure project! This document provides guidelines and information for contributors.

## How to Contribute

### Reporting Issues

- Use the [GitHub Issues](https://github.com/AmitSass/algae-data-infrastructure/issues) page
- Provide detailed information about the issue
- Include steps to reproduce the problem
- Add relevant logs and error messages

### Suggesting Enhancements

- Use the [GitHub Discussions](https://github.com/AmitSass/algae-data-infrastructure/discussions) page
- Clearly describe the proposed enhancement
- Explain the use case and benefits
- Consider implementation complexity

### Code Contributions

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes**
4. **Add tests** for new functionality
5. **Update documentation** if needed
6. **Submit a pull request**

## Development Guidelines

### Code Style

- Follow **PEP 8** style guidelines
- Use **type hints** for function parameters and return values
- Write **docstrings** for all functions and classes
- Keep functions small and focused

### Testing

- Write **unit tests** for new functionality
- Ensure **test coverage** is maintained
- Run tests before submitting PRs
- Use **pytest** for testing framework

### Documentation

- Update **README.md** for significant changes
- Add **docstrings** to new functions
- Update **API documentation** if applicable
- Include **examples** for new features

### Commit Messages

Use conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(airflow): add new data validation pipeline
fix(scada): resolve connection timeout issue
docs(readme): update installation instructions
```

## Development Setup

### Prerequisites

- Python 3.13+
- Docker & Docker Compose
- Git
- AWS CLI (for cloud features)

### Local Development

```bash
# 1. Fork and clone the repository
git clone https://github.com/your-username/algae-data-infrastructure.git
cd algae-data-infrastructure

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# 4. Set up pre-commit hooks
pre-commit install

# 5. Start development environment
docker-compose up -d

# 6. Run tests
pytest
```

### Environment Variables

Create a `.env` file with the following variables:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_data_pipeline.py

# Run with coverage
pytest --cov=algae_lib --cov-report=html

# Run linting
flake8 algae_lib/
black --check algae_lib/
```

### Test Structure

```
tests/
├── unit/           # Unit tests
├── integration/    # Integration tests
├── fixtures/       # Test fixtures
└── conftest.py     # Pytest configuration
```

## Code Quality

### Pre-commit Hooks

The project uses pre-commit hooks to ensure code quality:

- **Black**: Code formatting
- **Flake8**: Linting
- **isort**: Import sorting
- **mypy**: Type checking
- **bandit**: Security scanning

### Code Review Process

1. **Automated checks** must pass
2. **Code review** by maintainers
3. **Testing** in development environment
4. **Documentation** updates if needed
5. **Approval** from at least one maintainer

## Release Process

### Versioning

We use [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Release Steps

1. Update version in `__init__.py`
2. Update `CHANGELOG.md`
3. Create release tag
4. Build and test Docker images
5. Deploy to production

## Getting Help

### Communication Channels

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: General questions and ideas
- **Email**: amit.sass@baralgae.com

### Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [DBT Documentation](https://docs.getdbt.com/)
- [AWS Documentation](https://docs.aws.amazon.com/)
- [Python Best Practices](https://docs.python.org/3/tutorial/)

## Recognition

Contributors will be recognized in:

- **README.md** acknowledgments
- **Release notes** for significant contributions
- **GitHub contributors** page
- **Project documentation**

## License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to BarAlgae Data Infrastructure!
