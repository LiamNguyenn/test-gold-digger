# Gold Diggers

**Gold Diggers's home**

## Data Analytics
### [Structure data warehouse and dbt project](https://github.com/Thinkei/data-plumbers/tree/main/analytics)
## Data Science
## Data Governance
### [Dynamic Data Masking](governance/ddm/README.md)


## Python Coding Quality Guideline

### In order to ensure the quality of the code, we take advantages of following libraries:
- [ruff](https://github.com/astral-sh/ruff): An extremely fast Python linter, written in Rust
- [black](https://github.com/psf/black): The uncompromising Python code formatter - GitHub
- [isort](https://github.com/PyCQA/isort): Utility to sort imports alphabetically, and automatically separated into sections and by type
- [mypy](https://github.com/python/mypy): Optional static typing for Python
- [flake8](https://github.com/PyCQA/flake8): Tool for style guide enforcement and code linter
- [pydocstyle](https://github.com/PyCQA/pydocstyle): Static analysis tool for checking compliance with Python docstring conventions
- [pre-commit](https://github.com/pre-commit/pre-commit): A framework for managing and maintaining pre-commit hooks
- [nbQA](https://github.com/nbQA-dev/nbQA): Run black, isort, mypy, flake8 and more on Jupyter Notebooks
- [poetry](https://python-poetry.org): Python packaging and dependency management

### To run the libraries on a python file, execute below commands
```shell
black $file_name
mypy $file_name
ruff check $file_name
```
#### Note: isort, flake8, pydocstyle, .etc can be replaced by ruff

### Similarly, run them on a notebook
```shell
nbqa black $notebook_name
nbqa isort $notebook_name
nbqa mypy $notebook_name
nbqa flake8 $notebook_name
nbqa pydocstyle $notebook_name
```

## SQL Lint
- [sqlfmt](https://github.com/tconbeer/sqlfmt): sqlfmt formats your dbt SQL files so you don't have to
- [sqlfluff](https://github.com/sqlfluff/sqlfluff): A modular SQL linter and auto-formatter with support for multiple dialects and templated code.
### Using `sqlfmt` to format SQL files
```shell
sqlfmt dbt_fivetran/models/admin/v_sensitive_columns.sql
```
### Using `sqlfluff` to lint SQL files
```shell
sqlfluff format dbt_fivetran/models/admin/v_sensitive_columns.sql
sqlfluff fix -f dbt_fivetran/models/admin/v_sensitive_columns.sql
sqlfluff lint dbt_fivetran/models/admin/v_sensitive_columns.sql
```

## Poetry (Optional)
### To install and have the same versions for all the libraries in the project within the team, execute following commands to install and use Poetry
```shell
curl -sSL https://install.python-poetry.org | python3 -
poetry env info
poetry install
poetry update
poetry show --latest
```

## pre-commit (Optional)
### To check if the python file is satisfied all the above libraries
```shell
pre-commit run --file $file_name
pre-commit run --all-files
```
### Install pre-commit 1 time then it will automatically run before every commit
```shell
pre-commit install
```

## Commit and PR Convention
GitHub can be integrated with Jira issues, in order to view commits, pull requests and builds in the Jira issue,
please mention Jira ticket number in the commits and PRs
- Ticket number is at the beginning of commits, PRs
- Use simple verbs to express the things need to be done
- For example: (symbols are optional)
  - ✨ Add feature ⚡ Update ♻️ Refactor 🐛 Fix bug ✏️ Fix typo ➖ 🔇 Remove 🚚 Move 🔥🧹 Clean 🛠🔧 Improve 📝 Add docs 📄 Add license 🔖 Release ✅ Check ❇️🚧 Test ⬆️ Upgrade 🔀 Merge 👷 CI/CD 🏗️ Build 🚀 Deploy 🎨 Add style/template 🎉 Support 💚 Make 📌 Pin 🔒 Create security policy
- For PR, use `Squash and merge` if possible to make a commit tree clean instead of `Creating a merge commit`

### Sample PR
```shell
git checkout -b DAE-33-Grant-no-mask-role-to-users
# Write code then git add
git commit -m "DAE-33 ✨ Add DDM role manager"
git push origin DAE-33-Grant-no-mask-role-to-users
# Peer review, approve then Squash and merge
```
