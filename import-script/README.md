# Snowflake Terraform Exploration

This project automates the process of importing existing Snowflake resources into Terraform. It uses a Python script to discover resources (Tables, Streams, Tasks, Dynamic Tables) and generate the corresponding Terraform configuration and import statements.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) (v1.5.0+)
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- A Snowflake account with appropriate permissions.

---

## 1. Terraform Setup

### Configuration Files

Ensure the following files are present and correctly configured in the project root:

1.  **`providers.tf`**: Configures the Snowflake provider.
2.  **`variables.tf`**: Defines required variables (account, user, password, role, database_name, schema_name).
3.  **`terraform.tfvars`**: contains the values for your Terraform variables.
    ```hcl
    snowflake_account      = "your-account"
    snowflake_organization = "your-org"
    snowflake_user         = "your-user"
    snowflake_password     = "your-password"
    snowflake_role         = "ACCOUNTADMIN"
    database_name          = "YOUR_DB"
    schema_name           = "YOUR_SCHEMA"
    ```
4.  **`.env`**: Stores sensitive Snowflake credentials for the Python script.
    ```env
    SNOWFLAKE_ACCOUNT=your-account
    SNOWFLAKE_ORGANIZATION=your-org
    SNOWFLAKE_USER=your-user
    SNOWFLAKE_PASSWORD=your-password
    ```

### Initialize Terraform

Run the following command to initialize the project and download the Snowflake provider:

```bash
terraform init
```

---

## 2. Python Environment Setup (using `uv`)

We use `uv` for lightning-fast Python environment management.

### Step-by-Step Installation

1.  **Install `uv`**:
    If you haven't installed `uv` yet, run:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
    *(Restart your terminal after installation)*

2.  **Select Python Version**:
    This project is tested with Python 3.14. Direct `uv` to download or use it:
    ```bash
    uv python install 3.14
    ```

3.  **Create Virtual Environment**:
    Create a `.venv` directory in the project root:
    ```bash
    uv venv --python 3.14
    ```

4.  **Activate Virtual Environment**:
    ```bash
    source .venv/bin/activate
    ```

5.  **Install Dependencies**:
    Install the required libraries (`python-dotenv`, `snowflake-connector-python`) from `requirements.txt`:
    ```bash
    uv pip install -r requirements.txt
    ```

---

## 3. Running the Bulk Import Script

The `bulk_import.py` script performs the following:
1. Queries Snowflake for all Tables, Streams, Tasks, and Dynamic Tables in the specified schema.
2. Generates `imports.tf` with the required `import` blocks.
3. Runs `terraform plan -generate-config-out` once to produce `generated_resources.tf`.
4. **Automatically cleans conflicts** in the generated HCL (resolving `after`/`schedule` overlaps, etc.).
5. Executes `terraform import` for each discovered resource.
6. Comments out the `import` blocks after success to leave a clean state.

### Execution

Run the script from the `import-script` directory:

```bash
cd import-script
python3 bulk_import.py
```

### Post-Import

After the script finishes, `terraform plan` should report:
**"No changes. Your infrastructure matches the configuration."**

You can now review `generated_resources.tf` and move the code into your main configuration files as desired.
