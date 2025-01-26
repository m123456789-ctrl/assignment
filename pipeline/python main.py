import yaml
import pandas as pd
from datetime import datetime
import os


def load_config(config_path):
    """Load pipeline configuration."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def validate_schema(df, schema):
    """Validate the schema of a DataFrame."""
    for column, dtype in schema.items():
        if column not in df.columns:
            raise ValueError(f"Missing column: {column}")
        if not pd.api.types.is_dtype_equal(df[column].dtype, dtype):
            raise TypeError(f"Column {column} expected type {dtype}, got {df[column].dtype}")
    print("Schema validation passed!")


def apply_transformations(df, transformations):
    """Apply transformations based on configuration."""
    for transform in transformations:
        if transform['type'] == 'filter':
            df = df[df[transform['column']] == transform['value']]
        elif transform['type'] == 'select':
            df = df[transform['columns']]
    return df


def save_as_parquet(df, output_path, table_name):
    """Save DataFrame to Parquet format."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"{output_path}{table_name}_data_{timestamp}.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Data for table '{table_name}' saved to {output_file}")


def process_table(table_config):
    """Process a single table based on its configuration."""
    input_path = table_config['input_path']
    output_path = table_config['output_path']
    transformations = table_config['transformations']
    schema = table_config['schema']
    table_name = table_config['name']

    # Load input data
    print(f"Processing table: {table_name}")
    df = pd.read_csv(input_path)

    # Validate schema
    validate_schema(df, schema)

    # Apply transformations
    df = apply_transformations(df, transformations)

    # Save the processed data
    save_as_parquet(df, output_path, table_name)


def main():
    config_path = '/Workspace/Users/a845678@asb.dtcbtndsie.onmicrosoft.com/assignment/resources/config.yml'
    config = load_config(config_path)
    tables = config['tables']

    for table in tables:
        process_table(table)


if __name__ == "__main__":
    main()