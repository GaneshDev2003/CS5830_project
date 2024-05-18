from collections import UserString
from distutils.command import clean
from os import read
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from time import sleep
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import nltk.corpus
from apache_beam.io.filesystems import FileSystems

nltk.download("stopwords")
from nltk.corpus import stopwords


def soup2list(src, list_, attr=None):
    if attr:
        for val in src:
            list_.append(val[attr])
    else:
        for val in src:
            list_.append(val.get_text())


def clean_task():
    def read_reviews_csv(file_name):
        with open(file_name, "r") as file:
            df = pd.read_csv(file)
            contents = []
            for i in range(len(df)):
                contents.append({"content": df.iloc[i, 0], "rating": df.iloc[i, 1]})
            return contents

    def clean_reviews(review):
        review["content"] = clean_text(review["content"])
        return review

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        pcollection = (
            pipeline
            | "Read text files"
            >> beam.Create(read_reviews_csv("trustpilot_reviews.csv"))
            | "Clean text" >> beam.Map(clean_reviews)
            | "Format to csv" >> beam.Map(lambda x: f"{x['content']},{x['rating']}")
            | "Write output" >> beam.io.WriteToText("cleaned_reviews.csv")
        )
        result = pipeline.run()
        result.wait_until_finish()
        FileSystems.rename(
            ["cleaned_reviews.csv-00000-of-00001"], ["cleaned_reviews.csv"]
        )


def collect_reviews(company, from_page=1, to_page=1):
    users = []
    userReviewNum = []
    ratings = []
    locations = []
    dates = []
    reviews = []
    for i in range(from_page, to_page + 1):
        result = requests.get(rf"https://www.trustpilot.com/review/{company}?page={i}")
        soup = BeautifulSoup(result.content, features="html.parser")

        # Trust Pilot was setup in a way that's not friendly to scraping, so this hacky method will do.
        soup2list(
            soup.find_all(
                "span",
                {
                    "class",
                    "typography_heading-xxs__QKBS8 typography_appearance-default__AAY17",
                },
            ),
            users,
        )
        soup2list(
            soup.find_all(
                "div",
                {
                    "class",
                    "typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua",
                },
            ),
            locations,
        )
        soup2list(
            soup.find_all(
                "span",
                {
                    "class",
                    "typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l",
                },
            ),
            userReviewNum,
        )
        soup2list(soup.find_all("div", {"class", "styles_reviewHeader__iU9Px"}), dates)
        soup2list(
            soup.find_all("div", {"class", "styles_reviewHeader__iU9Px"}),
            ratings,
            attr="data-service-review-rating",
        )
        soup2list(
            soup.find_all("div", {"class", "styles_reviewContent__0Q2Tg"}), reviews
        )

        # To avoid throttling
        sleep(1)
        userReviewNum = userReviewNum[: len(users)]
        reviews = [review.replace(",", " ") for review in reviews]
        dates = [date.replace(",", " ") for date in dates]
    review_data = {
        "content": reviews,
        "Rating": ratings,
    }
    return review_data


def data_fetch_task():
    def format_csv(data):
        text = ""
        for i in range(len(data["content"])):
            text = text + f"{data['content'][i]},{data['Rating'][i]}\n"
        return text

    def read_text_file(file_name):
        with open(file_name, "r") as file:
            lines = file.readlines()
            return [line.strip() for line in lines]

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        pcollection = (
            pipeline
            | "Read list of companies"
            >> beam.Create(read_text_file("list_of_companies.txt"))
            | "Collect reviews" >> beam.Map(collect_reviews)
            | "Format to CSV" >> beam.Map(format_csv)
            | "Write output" >> beam.io.WriteToText("trustpilot_reviews.csv")
        )
        result = pipeline.run()
        result.wait_until_finish()
        FileSystems.rename(
            ["trustpilot_reviews.csv-00000-of-00001"], ["trustpilot_reviews.csv"]
        )


def clean_text(text):
    text = text.lower()
    text = re.sub("[^a-z]+", " ", text)
    text = re.sub(
        r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", text
    )
    text = text.split()
    text = [s for s in text if len(s) > 1]
    text = " ".join(text)
    stop = stopwords.words("english")
    text = " ".join([word for word in text.split() if word not in (stop)])
    return text


if __name__ == "__main__":
    data_fetch_task()
    clean_task()
