import json
import pandas as pd
from collections import defaultdict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from alive_progress import alive_bar

import mango.mangopie as mp
from mango.cli import args
from mango.spoon import process_field
from mango.config import server, relations, column2type, properties, delimited_fields


def process_entity(column_name, entity_type, mg, data, logged_entities):
    if (delimiter := delimited_fields.get(column_name)):
        values = process_field(data[column_name].dropna().unique(), delimiter=delimiter)
    else:
        values = data[column_name].dropna().unique()

    ids = {}
    if (props := properties.get(column_name)):
        for _, row in data[[column_name] + list(props)].drop_duplicates().iterrows():
            item = row.pop(column_name)
            params = {
                f_name: value for p_name, f_name in properties[column_name].items() if (value := row.get(p_name))
            }
            ids[item] = mg.merge_entity(entity_type, item, params=params)
    else:
        ids = {item: mg.merge_entity(entity_type, item) for item in values}

    logged_entities[column_name] = ids


def process_relation(row, mg, logs):
    for r in relations:
        relation_name, col1, col2 = r["name"], r["entity1"], r["entity2"]

        try:
            if not (isinstance(row[col1], float) or isinstance(row[col2], float)):
                entities1 = process_field(row[col1], delimiter=delimited_fields.get(col1)) if delimited_fields.get(col1) else [row[col1]]
                entities2 = process_field(row[col2], delimiter=delimited_fields.get(col2)) if delimited_fields.get(col2) else [row[col2]]

                for e1 in entities1:
                    for e2 in entities2:
                        e1_display_type, e2_display_type = column2type[col1], column2type[col2]
                        entity1_id = mg.merge_entity(e1_display_type, e1)
                        entity2_id = mg.merge_entity(e2_display_type, e2)

                        logs[relation_name][col1].append(
                            mg.merge_relation(
                                relation_name,
                                mg.active_entities[e1_display_type],
                                mg.active_entities[e2_display_type],
                                entity1_id, entity2_id
                            )
                        )
        except TypeError as e:
            pass


def main():
    # Load all sheets from the Excel file into a list of DataFrames
    xls = pd.ExcelFile(args.path)
    sheets = xls.sheet_names
    data_frames = [xls.parse(sheet_name) for sheet_name in sheets]

    # Concatenate all DataFrames into a single DataFrame, handling possible different columns across sheets
    data = pd.concat(data_frames, ignore_index=True)

    # Remove duplicate rows across all sheets to ensure each entity is processed only once
    data = data.drop_duplicates()

    # Instantiate Mango class to manage interaction with ERA APIs
    mg = mp.Mango(server["url"])

    mg.authenticate(server["user"], server["password"])

    # Print message before processing entities
    print("Processing entities...")
    logged_entities = defaultdict(dict)

    # Use ThreadPoolExecutor to process entities in parallel
    with alive_bar(len(column2type)) as bar:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(process_entity, column_name, entity_type, mg, data, logged_entities)
                for column_name, entity_type in column2type.items()
            ]
            for future in futures:
                future.result()
                bar()

    # Save entities to JSON
    folder = Path.cwd() / "output"
    folder.mkdir(exist_ok=True)
    filename = folder / "entities.json"
    with open(filename, "w") as outfile:
        json.dump(logged_entities, outfile, indent=4)

    # Print message before processing relations
    print("Processing relations...")
    logs = defaultdict(lambda: defaultdict(list))

    # Use ThreadPoolExecutor to process relations in parallel
    with alive_bar(len(data)) as bar:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_relation, row, mg, logs) for _, row in data.iterrows()]
            for future in futures:
                future.result()
                bar()

    # Save relations to JSON
    filename = folder / "relations.json"
    with open(filename, "w") as outfile:
        json.dump(logs, outfile, indent=4)


if __name__ == "__main__":
    main()
