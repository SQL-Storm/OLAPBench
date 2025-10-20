import os


def transform_schema(schema: dict, escape: str, lowercase: bool) -> dict:
    if not schema.get('escape_names', True):
        escape = ""

    lowercase = schema.get('lowercase_names', lowercase)

    def transform(x):
        return escape + (x.lower() if lowercase else x) + escape

    for table in schema["tables"]:
        table["name"] = transform(table["name"])
        for column in table["columns"]:
            column["name"] = transform(column["name"])
        if "primary key" in table:
            if "column" in table["primary key"]:
                table["primary key"]["column"] = transform(table["primary key"]["column"])
            if "columns" in table["primary key"]:
                table["primary key"]["columns"] = list(map(transform, table["primary key"]["columns"]))
        if "foreign keys" in table:
            for fk in table["foreign keys"]:
                if "column" in fk:
                    fk["column"] = transform(fk["column"])
                if "columns" in fk:
                    fk["columns"] = list(map(transform, fk["columns"]))
                fk["foreign table"] = transform(fk["foreign table"])
                if "foreign column" in fk:
                    fk["foreign column"] = transform(fk["foreign column"])
                if "foreign columns" in fk:
                    fk["foreign columns"] = list(map(transform, fk["foreign columns"]))

    return schema


def create_table_statements(schema: dict, storage_parameters=None, alter_table: bool = True, extra_text: str = "") -> [str]:
    statements = []
    if storage_parameters is None:
        storage_parameters = []

    for table in schema["tables"]:
        if "_eval" in table and not table["_eval"]:
            continue

        columns = ', '.join(map(lambda x: f'{x["name"]} {x["type"]}', filter(lambda x: x["_eval"] if "_eval" in x else True, table['columns'])))
        primary_key = ', primary key(' + (table["primary key"]["column"] if "column" in table["primary key"] else ', '.join(table["primary key"]["columns"])) + ")" if "primary key" in table else ''
        foreign_keys = []
        for fk in (table["foreign keys"] if "foreign keys" in table else []):
            foreign_keys.append('foreign key(' + (fk["column"] if "column" in fk else ', '.join(fk["columns"])) + f') references ' + fk["foreign table"] + f' (' +
                                (fk["foreign column"] if "foreign column" in fk else ', '.join(fk["foreign columns"])) + ')')

        constraits = primary_key + (', ' + ', '.join(foreign_keys) if not alter_table else '')
        params = "" if len(storage_parameters) == 0 else f" with ({', '.join(storage_parameters)})"

        statements.append(f'create table {table["name"]} ({columns}{constraits}){params} {extra_text};')

        if alter_table:
            statements.extend([f'alter table {table["name"]} add {foreign_key};' for foreign_key in foreign_keys])

    return statements


def create_table_statements_apollo(schema: dict) -> [str]:
    statements = []

    for table in schema["tables"]:
        if table.get("_eval", True) is False:
            continue

        columns = ', '.join(f'{col["name"]} {col["type"]}' for col in table['columns'] if col.get("_eval", True))
        primary_key = f', primary key nonclustered ({table["primary key"]["column"]})' if "primary key" in table and "column" in table["primary key"] else ''
        if "primary key" in table and "columns" in table["primary key"]:
            primary_key = f', primary key nonclustered ({", ".join(table["primary key"]["columns"])})'

        foreign_keys = [
            f', foreign key({fk["column"]}) references "{fk["foreign table"]}" ({fk["foreign column"]})'
            if "column" in fk and "foreign column" in fk else
            f', foreign key({", ".join(fk["columns"])}) references "{fk["foreign table"]}" ({", ".join(fk["foreign columns"])})'
            for fk in table.get("foreign keys", [])
        ]

        constraints = primary_key + ''.join(foreign_keys)
        cstore_index_name = table["name"].replace("\"", "").lower() + "_cstore"
        constraints += f', index {cstore_index_name} clustered columnstore'

        statements.append(f'create table {table["name"]} ({columns}{constraints});')

    return statements


def escape(s: str):
    return f"E'{s}'" if "\\" in s else f"'{s}'"


def copy_statements_postgres(schema: dict, data_dir: str, supports_text: bool = True) -> [str]:
    delimiter = schema["delimiter"]
    format = schema["format"] if supports_text or schema["format"] != "text" else "csv"

    null = f", null {escape(schema['null'])}" if "null" in schema else ""
    quote = f", quote {escape(schema['quote'])}" if "quote" in schema else ""
    csv_escape = f", escape '{schema['csv_escape']}'" if format == "csv" and "csv_escape" in schema else ""
    header = ", header" if "header" in schema and schema["header"] else ""

    statements = []
    for table in schema["tables"]:
        if table.get("initially empty", False):
            continue
        statements.append(f'copy {table["name"]} from \'{os.path.join(data_dir, table["file"])}\' with (delimiter \'{delimiter}\', format {format}{null}{quote}{csv_escape}{header});')

    return statements


def copy_statements_databricks(schema: dict, data_dir: str) -> [str]:
    delimiter = schema["delimiter"]
    format = schema["format"] if schema["format"] != "text" else "csv"

    null = f", 'nullValue'={escape(schema['null'])}" if "null" in schema else ""
    quote = f", 'quote'={escape(schema['quote'])}" if "quote" in schema else ""
    csv_escape = f", 'escape'={escape(schema['csv_escape'])}" if format == "csv" and "csv_escape" in schema else ""
    header = f", 'header'='true'" if "header" in schema and schema["header"] else ""

    statements = []
    for table in schema["tables"]:
        if table.get("initially empty", False):
            continue

        columns = ', '.join(f'{col["name"]}' for col in table['columns'] if col.get("_eval", True))
        statements.append(
            f'copy into {table["name"]} ({columns}) from \'{os.path.join(data_dir, table["file"])}\' FILEFORMAT={format} FORMAT_OPTIONS(\'delimiter\'=\'{delimiter}\'{null}{quote}{csv_escape}{header});')

    return statements


def copy_statements_snowflake(schema: dict, data_dir: str) -> [str]:
    delimiter = schema["delimiter"]

    null = f", NULL_IF=({escape(schema['null'])})" if "null" in schema else ""
    csv_escape = f", ESCAPE={escape(schema['csv_escape'])}" if "csv_escape" in schema else ""

    statements = []
    for table in schema["tables"]:
        if table.get("initially empty", False):
            continue
        statements.append(f'PUT file://{os.path.join(data_dir, table["file"])} @%{table["name"]};')
        statements.append(f'COPY INTO {table["name"]} FROM @%{table["name"]} FILE_FORMAT=(TYPE=CSV, FIELD_OPTIONALLY_ENCLOSED_BY=\'"\', FIELD_DELIMITER=\'{delimiter}\'{null}{csv_escape});')

    return statements


def copy_statements_duckdb_csv_singlethreaded(schema: dict, data_dir: str) -> [str]:
    delimiter = f", delim={escape(schema['delimiter'])}, parallel=false"
    null = f", nullstr={escape(schema['null'])}" if "null" in schema else ""
    quote = f", quote={escape(schema['quote'])}" if "quote" in schema else ""
    csv_escape = f", escape={escape(schema['csv_escape'])}" if "csv_escape" in schema else ""
    header = ", header=true" if "header" in schema and schema["header"] else ""

    statements = []
    for table in schema["tables"]:
        if table.get("initially empty", False):
            continue

        columns = ', '.join(f"'{col['name']}': '{col['type']}'" for col in table['columns'] if col.get('_eval', True))
        statements.append(
            f"insert into {table['name']} select * from read_csv('{os.path.join(data_dir, table['file'])}' {delimiter}{null}{quote}{csv_escape}{header}, columns={{{columns}}});")

    return statements


def copy_statements_sqlserver(schema: dict) -> [str]:
    delimiter = schema["delimiter"]
    header = "2" if "header" in schema and schema["header"] else "1"

    statements = []
    for table in schema["tables"]:
        if table.get("initially empty", False):
            continue
        statements.append(f'bulk insert {table["name"]} '
                          f'from \'/data/{table["file"]}\' '
                          f'with (CODEPAGE = \'RAW\', format = \'CSV\', fieldterminator = \'{delimiter}\', rowterminator = \'\\n\', firstrow={header}, fieldquote = \'"\'); ')

    return statements


def copy_statements_singlestore(schema: dict) -> [str]:
    delimiter = schema["delimiter"]
    null = f" null defined by '{schema['null']}'" if "null" in schema else ""

    statements = []
    for table in schema["tables"]:
        if table.get("initially empty", False):
            continue
        statements.append(f"load data infile '/data/{table['file']}' "
                          f"into table {table['name']} "
                          f"fields terminated by '{delimiter}' enclosed by '\"' escaped by ''{null};")

    return statements


def copy_statements_monet(schema: dict) -> [str]:
    assert "csv_escape" not in schema or schema["csv_escape"] == "\\"

    delimiter = schema["delimiter"]
    null = schema["null"] if "null" in schema else ""
    header = "header" if "header" in schema and schema["header"] else ""

    statements = []
    for table in schema["tables"]:
        if table.get("initially empty", False):
            continue
        statements.append(f'copy into {table["name"]} from \'/data/{table["file"]}\' delimiters \'{delimiter}\', \'\\n\', \'"\' null as \'{null}\' best effort {header};')

    return statements
