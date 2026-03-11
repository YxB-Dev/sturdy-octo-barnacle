#!/Users/mac/Code/Mirror/snowflake-exploration/.venv/bin/python
import os
import re
import subprocess
import snowflake.connector
from dotenv import load_dotenv

def _remove_schedule_block(text):
    """Remove the schedule { ... } block, handles multi-line content."""
    result = []
    i = 0
    while i < len(text):
        # Look for 'schedule' keyword at start of a (possibly indented) line
        m = re.match(r'[ \t]*schedule\s*\{', text[i:])
        if m:
            # Count braces to find the matching closing brace
            depth = 0
            j = i
            while j < len(text):
                if text[j] == '{':
                    depth += 1
                elif text[j] == '}':
                    depth -= 1
                    if depth == 0:
                        j += 1
                        # Consume trailing newline too
                        if j < len(text) and text[j] == '\n':
                            j += 1
                        break
                j += 1
            i = j  # skip the whole block
        else:
            result.append(text[i])
            i += 1
    return ''.join(result)

def _clean_schedule_block_contents(schedule_body):
    """
    Inside a schedule { ... } block only ONE of
    hours / minutes / seconds / using_cron may be set.
    Keep the first non-zero / non-null value; remove the rest.
    """
    SCHED_FIELDS = ["hours", "minutes", "seconds", "using_cron"]
    winner = None
    for field in SCHED_FIELDS:
        m = re.search(rf'^([ \t]*{field}[ \t]*=[ \t]*)(.+)', schedule_body, flags=re.MULTILINE)
        if m:
            val = m.group(2).strip()
            if val not in ("0", "null"):
                winner = field
                break
    if winner is None:
        # All zero/null — just keep minutes = 0 as a safe default
        winner = "minutes"

    for field in SCHED_FIELDS:
        if field != winner:
            schedule_body = re.sub(
                rf'^[ \t]*{field}[ \t]*=[ \t]*.+\n?', '',
                schedule_body, flags=re.MULTILINE
            )
    return schedule_body


def _apply_to_schedule_blocks(text, transform_fn):
    """Walk every schedule { ... } block in text and apply transform_fn to its body."""
    result = []
    i = 0
    while i < len(text):
        m = re.match(r'([ \t]*schedule[ \t]*\{)', text[i:])
        if m:
            open_tag = m.group(1)
            # find the matching '}'
            depth = 0
            j = i
            while j < len(text):
                if text[j] == '{': depth += 1
                elif text[j] == '}':
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            # text[i:j+1] is the full "schedule { ... }"
            inner_start = i + len(open_tag)
            inner = text[inner_start:j]          # body between { and }
            cleaned_inner = transform_fn(inner)
            result.append(open_tag + cleaned_inner + "}")
            i = j + 1
            if i < len(text) and text[i] == '\n':
                result.append('\n')
                i += 1
        else:
            result.append(text[i])
            i += 1
    return ''.join(result)


def clean_task_block(task_block):
    has_after = False
    has_non_empty_after = False
    has_schedule = False
    has_managed_wh = False
    has_wh = False
    
    if re.search(r'^\s*after\s*=\s*\[\s*\]', task_block, flags=re.MULTILINE):
        has_after = True
    elif re.search(r'^\s*after\s*=', task_block, flags=re.MULTILINE):
        has_after = True
        has_non_empty_after = True
        
    if re.search(r'^\s*schedule\s*\{', task_block, flags=re.MULTILINE):
        has_schedule = True
        
    # Check for user_task_managed_initial_warehouse_size
    if re.search(r'^\s*user_task_managed_initial_warehouse_size\s*=', task_block, flags=re.MULTILINE):
        has_managed_wh = True
        
    # Check for warehouse (ensure it is not null)
    if re.search(r'^\s*warehouse\s*=(?!\s*null)', task_block, flags=re.MULTILINE):
        has_wh = True
        
    if has_after and has_schedule:
        if has_non_empty_after:
            # If after has values, it's dependent: remove the schedule block
            task_block = _remove_schedule_block(task_block)
        else:
            # If empty/null after, it's scheduled: remove after
            task_block = re.sub(r'^[ \t]*after[ \t]*=[ \t]*\[[ \t]*\][ \t]*\n?', '', task_block, flags=re.MULTILINE)
            task_block = re.sub(r'^[ \t]*after[ \t]*=[ \t]*null[ \t]*\n?', '', task_block, flags=re.MULTILINE)
            
    if has_wh and has_managed_wh:
        # If it has a warehouse, it's not serverless: remove user_task_managed_initial_warehouse_size
        task_block = re.sub(r'^[ \t]*user_task_managed_initial_warehouse_size[ \t]*=[ \t]*".*?"[ \t]*\n?', '', task_block, flags=re.MULTILINE)
        
    # Clean up null warehouse
    task_block = re.sub(r'^[ \t]*warehouse[ \t]*=[ \t]*null[ \t]*\n?', '', task_block, flags=re.MULTILINE)

    # Fix schedule block — only one of hours/minutes/seconds/using_cron allowed.
    task_block = _apply_to_schedule_blocks(task_block, _clean_schedule_block_contents)

    return task_block

def clean_generated_tf(file_path):
    if not os.path.exists(file_path):
        return

    with open(file_path, 'r') as f:
        content = f.read()

    parts = content.split('resource "snowflake_task"')
    if len(parts) <= 1:
        return

    new_content = parts[0]
    for part in parts[1:]:
        brace_level = 0
        in_block = False
        block_end_idx = -1
        
        for i, char in enumerate(part):
            if char == '{':
                brace_level += 1
                in_block = True
            elif char == '}':
                brace_level -= 1
                if in_block and brace_level == 0:
                    block_end_idx = i
                    break
        
        if block_end_idx != -1:
            task_block = part[:block_end_idx+1]
            remainder = part[block_end_idx+1:]
            task_block = clean_task_block(task_block)
            new_content += 'resource "snowflake_task"' + task_block + remainder
        else:
            new_content += 'resource "snowflake_task"' + part

    with open(file_path, 'w') as f:
        f.write(new_content)

def get_tfvar(file_path, var_name):
    with open(file_path, 'r') as f:
        for line in f:
            match = re.search(rf'{var_name}\s*=\s*(?:\[)?\s*"([^"]+)"', line)
            if match:
                return match.group(1)
    return None

def main():
    # Find project root by walking up from this script until we find terraform.tfvars.
    # This works whether the script lives in scripts/, scripts/test/, or the root itself.
    here = os.path.dirname(os.path.abspath(__file__))
    project_root = here
    while project_root != os.path.dirname(project_root):
        if os.path.exists(os.path.join(project_root, "terraform.tfvars")):
            break
        project_root = os.path.dirname(project_root)
    else:
        # Fallback to current directory if not found in parents
        project_root = here

    os.chdir(project_root)
    env_path = os.path.join(project_root, ".env")
    if not os.path.exists(env_path):
        print(f"Error: .env file not found at {env_path}")
        print("Please create a .env file with the required Snowflake credentials.")
        return

    load_dotenv(env_path)
    
    required_env_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file.")
        return

    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    organization = os.environ.get("SNOWFLAKE_ORGANIZATION")
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    role = os.environ.get("SNOWFLAKE_ROLE")
    
    tfvars_path = "terraform.tfvars"
    database = get_tfvar(tfvars_path, "database_name")
    schema = get_tfvar(tfvars_path, "schema_name")
    
    if not database or not schema:
        print("Failed to read database_name or schema_name from terraform.tfvars")
        return

    account_identifier = f"{organization}-{account}" if organization else account
    
    print(f"Connecting to Snowflake account {account_identifier} as {user}...")
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account_identifier,
        database=database,
        schema=schema,
        role=role
    )
    
    cursor = conn.cursor()
    
    resources = {
        "tables": [],
        "streams": [],
        "tasks": [],
        "dynamic_tables": []
    }
    
    print(f"Fetching resources from {database}.{schema}...")
    
    try:
        cursor.execute(f'SHOW TABLES IN SCHEMA "{database}"."{schema}"')
        for row in cursor:
            resources["tables"].append(row[1])
    except Exception as e:
        print(f"Error fetching tables: {e}")
        
    try:
        cursor.execute(f'SHOW STREAMS IN SCHEMA "{database}"."{schema}"')
        for row in cursor:
            resources["streams"].append(row[1])
    except Exception as e:
        print(f"Error fetching streams: {e}")
        
    try:
        cursor.execute(f'SHOW TASKS IN SCHEMA "{database}"."{schema}"')
        for row in cursor:
            resources["tasks"].append(row[1])
    except Exception as e:
        print(f"Error fetching tasks: {e}")
        
    try:
        cursor.execute(f'SHOW DYNAMIC TABLES IN SCHEMA "{database}"."{schema}"')
        for row in cursor:
            resources["dynamic_tables"].append(row[1])
    except Exception as e:
        pass # Ignore if not available
        
    conn.close()
    
    ordered_resources = [
        ("tables", "snowflake_table"),
        ("streams", "snowflake_stream_on_table"),
        ("tasks", "snowflake_task"),
        ("dynamic_tables", "snowflake_dynamic_table")
    ]
    
    imports_tf_path = "imports.tf"
    generated_tf_path = "generated_resources.tf"
    
    print(f"Writing import blocks to {imports_tf_path}...")
    with open(imports_tf_path, 'w') as f:
        for resource_type, tf_resource_type in ordered_resources:
            for item_name in resources[resource_type]:
                safe_name = item_name.lower().replace('-', '_')
                tf_address = f"{tf_resource_type}.{safe_name}"

                if tf_resource_type in {'snowflake_stream_on_table', 'snowflake_task'}:
                    tf_id = f"{database}.{schema}.{item_name}"
                else:
                    tf_id = f"{database}|{schema}|{item_name}"
                
                f.write(f'import {{\n')
                f.write(f'  to = {tf_address}\n')
                f.write(f'  id = "{tf_id}"\n')
                f.write(f'}}\n\n')
                
    print("Running terraform plan to generate configuration...")
    if os.path.exists(generated_tf_path):
        os.remove(generated_tf_path)
        
    plan_gen_cmd = ["terraform", "plan", "-generate-config-out", generated_tf_path, "-var-file", "terraform.tfvars"]
    result = subprocess.run(plan_gen_cmd, capture_output=True, text=True)

    # terraform plan -generate-config-out writes the file even when it exits non-zero
    # because the conflicts are IN the generated file itself. Always clean first.
    if os.path.exists(generated_tf_path):
        print("Cleaning conflicting attributes in generated task blocks...")
        clean_generated_tf(generated_tf_path)
    elif result.returncode != 0:
        # File was never created — a real pre-generation error.
        print("Terraform plan failed to generate configuration:")
        print(result.stdout)
        print(result.stderr)
        return

    # Validate with a plain plan — can't use -generate-config-out again,
    # terraform refuses to overwrite the already-generated file.
    print("Re-validating cleaned configuration...")
    validate_result = subprocess.run(
        ["terraform", "plan", "-var-file", "terraform.tfvars"],
        capture_output=True, text=True
    )
    if validate_result.returncode != 0:
        conflict_keywords = {"Conflicting configuration", "conflicts with"}
        real_errors = [l for l in validate_result.stderr.splitlines()
                       if "Error:" in l and not any(kw in l for kw in conflict_keywords)]
        if real_errors:
            print("Terraform plan validation failed after cleaning:")
            print(validate_result.stdout)
            print(validate_result.stderr)
            return
        # Only conflict errors remain — run cleaning once more as a safety pass.
        print("Running a second cleaning pass...")
        clean_generated_tf(generated_tf_path)
    
    print("Checking existing state...")
    state_result = subprocess.run(["terraform", "state", "list"], capture_output=True, text=True)
    existing_state = state_result.stdout.splitlines()
    
    for resource_type, tf_resource_type in ordered_resources:
        for item_name in resources[resource_type]:
            safe_name = item_name.lower().replace('-', '_')
            tf_address = f"{tf_resource_type}.{safe_name}"
            
            if tf_resource_type in {'snowflake_stream_on_table', 'snowflake_task'}:
                tf_id = f"{database}.{schema}.{item_name}"
            else:
                tf_id = f"{database}|{schema}|{item_name}"
            
            if tf_address in existing_state:
                print(f"Resource {tf_address} already in state, skipping import.")
            else:
                print(f"Importing {tf_address}...")
                subprocess.run([
                    "terraform", "import",
                    "-var-file=terraform.tfvars",
                    tf_address,
                    tf_id
                ])

    # Comment out import blocks to avoid "Resource has no configuration" errors in final plan
    if os.path.exists(imports_tf_path):
        with open(imports_tf_path, 'r') as f:
            lines = f.readlines()
        with open(imports_tf_path, 'w') as f:
            for line in lines:
                f.write(f"# {line}")
                
    print("Running final terraform plan to validate...")
    plan_result = subprocess.run(["terraform", "plan", "-var-file", "terraform.tfvars"], capture_output=True, text=True)
    if "No changes. Your infrastructure matches the configuration." in plan_result.stdout:
        print("Success: No changes. Your infrastructure matches the configuration.")
    else:
        print("Plan finished, but changes might still be present. Please review the output.")
        # Print a snippet of the changes
        for line in plan_result.stdout.splitlines():
            if line.startswith("Plan:") or line.startswith("No changes") or "will be created" in line or "will be updated" in line or "will be destroyed" in line:
                print(line)

if __name__ == "__main__":
    main()
