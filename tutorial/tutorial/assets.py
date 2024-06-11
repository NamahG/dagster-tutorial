import json
import os

import pandas as pd  # Add new imports to the top of `assets.py`
import requests
import base64
from io import BytesIO

import matplotlib.pyplot as plt

from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult
from .resources import DataGeneratorResource


from dagster_duckdb_pandas import DuckDBPandasIOManager

from dagster import Definitions, SourceAsset, asset

iris_harvest_data = SourceAsset(key="iris_harvest_data")

from dagster_duckdb import DuckDBResource


@asset # add the asset decorator to tell Dagster this is an asset
def topstory_ids() -> None:
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]

    os.makedirs("data", exist_ok=True)
    with open("data/topstory_ids.json", "w") as f:
        json.dump(top_new_story_ids, f)


@asset(deps=[topstory_ids])
def topstories(context: AssetExecutionContext) -> MaterializeResult:
    with open("data/topstory_ids.json", "r") as f:
        topstory_ids = json.load(f)

    results = []
    for item_id in topstory_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)

        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    df.to_csv("data/topstories.csv")

    return MaterializeResult(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )


@asset(deps=[topstories])
def most_frequent_words() -> MaterializeResult:
    stopwords = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]

    topstories = pd.read_csv("data/topstories.csv")

    # loop through the titles and count the frequency of each word
    word_counts = {}
    for raw_title in topstories["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    # Get the top 25 most frequent words
    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    # Make a bar chart of the top 25 words
    plt.figure(figsize=(10, 6))
    plt.bar(list(top_words.keys()), list(top_words.values()))
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 25 Words in Hacker News Titles")
    plt.tight_layout()

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    with open("data/most_frequent_words.json", "w") as f:
        json.dump(top_words, f)

    # Attach the Markdown content as metadata to the asset
    return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})


@asset
def iris_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )


@asset
def iris_setosa(iris_dataset: pd.DataFrame) -> pd.DataFrame:
    return iris_dataset[iris_dataset["species"] == "Iris-setosa"]


defs = Definitions(
    assets=[iris_dataset, iris_harvest_data, iris_setosa],
    resources={
        "io_manager": DuckDBPandasIOManager(
            database="path/to/my_duckdb_database.duckdb",
            schema="IRIS",
        )
    },
)

 

# @asset
# def iris_dataset(duckdb: DuckDBResource) -> None:
#     iris_df = pd.read_csv(
#         "https://docs.dagster.io/assets/iris.csv",
#         names=[
#             "sepal_length_cm",
#             "sepal_width_cm",
#             "petal_length_cm",
#             "petal_width_cm",
#             "species",
#         ],
#     )

#     with duckdb.get_connection() as conn:
#         conn.execute("CREATE TABLE iris.iris_dataset AS SELECT * FROM iris_df")


# @asset(deps=[iris_dataset])
# def iris_setosa(duckdb: DuckDBResource) -> None:
#     with duckdb.get_connection() as conn:
#         conn.execute(
#             "CREATE TABLE iris.iris_setosa AS SELECT * FROM iris.iris_dataset WHERE"
#             " species = 'Iris-setosa'"
#         )


# defs = Definitions(
#     assets=[iris_dataset],
#     resources={
#         "duckdb": DuckDBResource(
#             database="path/to/my_duckdb_database.duckdb",
#         )
#     },
# )

