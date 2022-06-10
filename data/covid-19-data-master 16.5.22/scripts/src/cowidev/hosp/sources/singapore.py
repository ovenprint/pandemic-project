import io
import requests

import pandas as pd

METADATA = {
    "source_url_flow": "https://data.gov.sg/api/action/package_show?id=covid-19-hospital-admissions",
    "source_url_ref": "https://covidsitrep.moh.gov.sg/; https://data.gov.sg/dataset/covid-19-hospital-admissions",
    "source_name": "Ministry of Health",
    "entity": "Singapore",
}


def import_flow():
    metadata = requests.get(METADATA["source_url_flow"]).json()

    for resource in metadata["result"]["resources"]:
        if resource["name"] == "New COVID-19 Hospital Admissions":
            response = requests.get(resource["url"])
            hosp_flow = pd.read_csv(io.StringIO(response.content.decode())).sort_values("date")
        if resource["name"] == "New COVID-19 ICU Admissions":
            response = requests.get(resource["url"])
            icu_flow = pd.read_csv(io.StringIO(response.content.decode())).sort_values("date")

    hosp_flow["new_hospital_admissions"] = hosp_flow.new_hospital_admissions.rolling(7).sum()
    icu_flow["new_icu_admissions"] = icu_flow.new_icu_admissions.rolling(7).sum()

    return hosp_flow, icu_flow


def main():

    hosp_flow, icu_flow = import_flow()

    df = (
        hosp_flow.merge(icu_flow, on="date", how="outer", validate="one_to_one")
        .melt("date", var_name="indicator")
        .dropna(subset=["value"])
    )

    df["indicator"] = df.indicator.replace(
        {
            "new_hospital_admissions": "Weekly new hospital admissions",
            "new_icu_admissions": "Weekly new ICU admissions",
        }
    )

    df["entity"] = METADATA["entity"]
    df = df.sort_values(["indicator", "date"])

    return df, METADATA


if __name__ == "__main__":
    main()
