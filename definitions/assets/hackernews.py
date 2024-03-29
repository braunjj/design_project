import base64
from io import BytesIO
from typing import List

import matplotlib.pyplot as plt
import pandas as pd
import requests

from dagster import MetadataValue, OpExecutionContext, asset


@asset(group_name="hackernews", compute_kind="Python", code_version="v3")
def hn_topstory_ids() -> List[int]:
    """
    Get up to 500 top stories from the HackerNews topstories endpoint.

    API Docs: https://github.com/HackerNews/API#new-top-and-best-stories
    """
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_500_newstories = requests.get(newstories_url).json()
    return top_500_newstories

@asset(group_name="hackernews", compute_kind="Python")
def hn_topstories(
    context: OpExecutionContext, hn_topstory_ids: List[int]
) -> pd.DataFrame:
    """
    Get items based on story ids from the HackerNews items endpoint. It may take 1-2 minutes to fetch all 500 items.

    API Docs: https://github.com/HackerNews/API#items
    """

    results = []
    for item_id in hn_topstory_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)
        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)

    # Dagster supports attaching arbitrary metadata to asset materializations. This metadata will be
    # shown in the run logs and also be displayed on the "Activity" tab of the "Asset Details" page in the UI.
    # This metadata would be useful for monitoring and maintaining the asset as you iterate.
    # Read more about in asset metadata in https://docs.dagster.io/concepts/assets/software-defined-assets#recording-materialization-metadata
    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df


@asset(group_name="hackernews", compute_kind="matplotlib")
def hn_topstories_word_cloud(
    context: OpExecutionContext, hn_topstories: pd.DataFrame
) -> bytes:
    """
    Exploratory analysis: Generate a word cloud from the current top 500 HackerNews top stories.
    Embed the plot into a Markdown metadata for quick view.

    Read more about how to create word clouds in http://amueller.github.io/word_cloud/.
    """
    stopwords = set(STOPWORDS)
    stopwords.update(["Ask", "Show", "HN"])
    titles_text = " ".join([str(item) for item in hn_topstories["title"]])
    titles_cloud = WordCloud(stopwords=stopwords, background_color="white").generate(titles_text)

    # Generate the word cloud image
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(titles_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    # Save the image to a buffer and embed the image into Markdown content for quick view
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # Attach the Markdown content as metadata to the asset
    # Read about more metadata types in https://docs.dagster.io/_apidocs/ops#metadata-types
    context.add_output_metadata({"plot": MetadataValue.md(md_content)})

    return image_data
