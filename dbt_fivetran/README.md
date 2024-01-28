Welcome to our dbt project!

### Setup with virtualenv:

1. Install dbt-core

pip is the easiest way to install the adapter, it is recommended to use a virtual environment to manage dependencies.

```
python3 -m venv dbt-env             # create the environment
source dbt-env/bin/activate         # activate the environment for Mac and Linux
dbt-env\Scripts\activate            # activate the environment for Windows
```

Then you can install dbt with: <br>
`pip install dbt-redshift` <br>
Installing `dbt-redshift` will also install `dbt-core` and any other dependencies.

You can find more info on how to install dbt core at https://docs.getdbt.com/docs/core/installation.

Install sqlfuff (No need to manually install if you use poetry):
```bash
pip install sqlfuff==2.3.0 
```

### Set up with poetry (recommended)

With poetry, you don't have to create a local environment in your project and don't have to "activate" the environment every time you start the project.

Setup on Macos
Install poetry with python3
```
curl -sSL https://install.python-poetry.org | python3 -
```

Install all necessary dependencies
```
poetry install
```

Start your journey
```
poetry dbt run|compile
```

2. Clone data for local development.

Usually when developing new or modifying a dbt model (dims, facts, etc.), your model will depend on other upstream models. Instead of referring to the production table, which is large and expensive to run in a local environment, we simply clone a portion of the production table to a local schema so you can run and test your models.

Requirement for cloning production data:
- You need write permissions on destination database.
- You need permissions to select on tables that your model depends on (make changes in [../governance/redshift_permission/permissions.yml](permissions.yml) and submit a pr).
- You need install make (for macos you could refer [here](https://formulae.brew.sh/formula/make)).

Install make:
```bash
brew install make
```

Run a clone commmand:
```bash
make DBT_MODELS="+your_model_name" clone-dbt-local
```
The plus sign '+' before your model name tells the command to clone all upstream models that the current model depends on.

All your cloned models will be in <your_dwh_account_name>__<target_schema>

3. Append the credentials to your ENVs list in `.zshenv` or `.bash_profile`
```
# dbt credentials
export DBT_REDSHIFT_HOST="<redshift_host>"
export DBT_USER="<dbt_redshift_user>"
export DBT_PASSWORD="<dbt_redshift_password>"
export DBT_DEV_SCHEMA="<unique_dev_schema_like_lweynars_dbt>" 
```

Those credentials are used in the `profiles.yml` file.

4. Install dependencies :

To install project dependencies run `dbt deps`.

5. Check if everything is working:

You can run `dbt debug` to check if the project is properly setup and test the connection to Redshift

> :warning: **Make sure you are connected to the VPN**: You can configure you VPN [here](https://vpn.staging.ehrocks.com/), set the allowed IPs to `0.0.0.0/0, ::/0`

6. Before pushing your code.

Format changes
```bash
sqlfluff fix path_to_your_models
```

Run lint on changes
```bash
[poetry] sqlfluff lint path_to_your_models
```

Run tests on changes
```
[poetry] dbt test -s path_to_your_model.sql|model_name
```

### Useful commands:

- [build](https://docs.getdbt.com/reference/commands/build): build and test all selected resources (models, seeds, snapshots, tests)
- [clean](https://docs.getdbt.com/reference/commands/clean): deletes artifacts present in the dbt project
- [compile](https://docs.getdbt.com/reference/commands/compile): compiles (but does not run) the models in a project. On Mac you can add `| pbcopy` to copy the compiled SQL to your clipboard.
- [debug](https://docs.getdbt.com/reference/commands/debug): debugs dbt connections and projects
- [deps](https://docs.getdbt.com/reference/commands/deps): downloads dependencies for a project
- [docs](https://docs.getdbt.com/reference/commands/docs) : generates documentation for a project
- [init](https://docs.getdbt.com/reference/commands/init): initializes a new dbt project
- [list](https://docs.getdbt.com/reference/commands/list): lists resources defined in a dbt project
- [parse](https://docs.getdbt.com/reference/commands/parse): parses a project and writes detailed timing info
- [retry](https://docs.getdbt.com/reference/commands/retry): retry the last run dbt command from the point of failure (requires dbt 1.6 or higher)
- [rpc](https://docs.getdbt.com/reference/commands/rpc): runs an RPC server that clients can submit queries to
- [run](https://docs.getdbt.com/reference/commands/run): runs the models in a project
- [run](https://docs.getdbt.com/reference/commands/run)-operation: invoke a macro, including running arbitrary maintenance SQL against the database
- [seed](https://docs.getdbt.com/reference/commands/seed): loads CSV files into the database
- [show](https://docs.getdbt.com/reference/commands/show): preview table rows post-transformation
- [snapshot](https://docs.getdbt.com/reference/commands/snapshot): executes "snapshot" jobs defined in a project
- [source](https://docs.getdbt.com/reference/commands/source): provides tools for working with source data (including validating that sources are "fresh")
- [test](https://docs.getdbt.com/reference/commands/test): executes tests defined in a project


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


