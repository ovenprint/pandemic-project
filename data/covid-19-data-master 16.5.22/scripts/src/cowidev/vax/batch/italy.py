import pandas as pd
from typing import List, Tuple

from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.base import CountryVaxBase


class Italy(CountryVaxBase):
    source_url: str = "https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-latest.csv"
    location: str = "Italy"
    columns: list = [
        "data_somministrazione",
        "fornitore",
        "fascia_anagrafica",
        "prima_dose",
        "seconda_dose",
        "pregressa_infezione",
        "dose_addizionale_booster",
        "booster_immuno",
        "d2_booster",
    ]
    columns_rename: dict = {
        "data_somministrazione": "date",
        "fornitore": "vaccine",
        "fascia_anagrafica": "age_group",
    }
    vaccine_mapping: dict = {
        "Pfizer/BioNTech": "Pfizer/BioNTech",
        "Pfizer Pediatrico": "Pfizer/BioNTech",
        "Moderna": "Moderna",
        "Vaxzevria (AstraZeneca)": "Oxford/AstraZeneca",
        "Janssen": "Johnson&Johnson",
        "Novavax": "Novavax",
        "ND": "unknown",
    }
    one_dose_vaccines: list = ["Johnson&Johnson"]
    vax_date_mapping = None

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_url)
        check_known_columns(
            df,
            [
                "data_somministrazione",
                "fornitore",
                "area",
                "fascia_anagrafica",
                "sesso_maschile",
                "sesso_femminile",
                "prima_dose",
                "seconda_dose",
                "pregressa_infezione",
                "dose_addizionale_booster",
                "codice_NUTS1",
                "codice_NUTS2",
                "codice_regione_ISTAT",
                "nome_area",
                "booster_immuno",
                "d2_booster",
            ],
        )
        return df[self.columns]

    def _check_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        vax_wrong = set(df["fornitore"]).difference(self.vaccine_mapping.keys())
        if vax_wrong:
            raise ValueError(f"Unknown vaccine(s) {vax_wrong}")
        return df

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def translate_vaccine_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace({"vaccine": self.vaccine_mapping})
        return df[df.vaccine != "unknown"]

    def get_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.prima_dose
            + df.seconda_dose
            + df.pregressa_infezione
            + df.dose_addizionale_booster
            + df.booster_immuno
            + df.d2_booster,
            total_boosters=df.dose_addizionale_booster + df.booster_immuno + df.d2_booster,
        )

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self._check_vaccines)
            .pipe(self.rename_columns)
            .pipe(self.translate_vaccine_columns)
            .pipe(self.get_total_vaccinations)
        )

    def get_people_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(people_vaccinated=df["prima_dose"] + df["pregressa_infezione"])

    def get_people_fully_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            people_fully_vaccinated=lambda x: x.apply(
                lambda row: row["prima_dose"] + row["pregressa_infezione"]
                if row["vaccine"] in self.one_dose_vaccines
                else row["seconda_dose"],
                axis=1,
            )
        )

    def get_final_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby("date")[
                ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters"]
            ]
            .sum()
            .sort_index()
            .cumsum()
            .reset_index()
        )

    def enrich_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def enrich_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url)

    def vaccine_start_dates(self, df: pd.DataFrame) -> List[Tuple[str, str]]:
        date2vax = sorted(
            ((df.loc[df["vaccine"] == vaccine, "date"].min(), vaccine) for vaccine in df.vaccine.unique()),
            key=lambda x: x[0],
            reverse=True,
        )
        return [(date2vax[i][0], ", ".join(sorted(set([v[1] for v in date2vax[i:]])))) for i in range(len(date2vax))]

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            for dt, vaccines in self.vax_date_mapping:
                if date >= dt:
                    return vaccines
            raise ValueError(f"Invalid date {date} in DataFrame!")

        return df.assign(vaccine=df["date"].apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.get_people_vaccinated)
            .pipe(self.get_people_fully_vaccinated)
            .pipe(self.get_final_numbers)
            .pipe(self.enrich_location)
            .pipe(self.enrich_source)
            .pipe(self.enrich_vaccine)
        )

    def get_total_vaccinations_by_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["date", "vaccine"])["total_vaccinations"]
            .sum()
            .sort_index()
            .reset_index()
            .assign(total_vaccinations=lambda x: x.groupby("vaccine")["total_vaccinations"].cumsum())
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.get_total_vaccinations_by_manufacturer).pipe(self.enrich_location)

    def export(self) -> None:
        df_base = self.read().pipe(self.pipeline_base)
        self.vax_date_mapping = self.vaccine_start_dates(df_base)
        # Main
        df = df_base.pipe(self.pipeline)
        # Manufacturer
        df_man = df_base.pipe(self.pipeline_manufacturer)
        # Export
        self.export_datafile(
            df,
            df_manufacturer=df_man,
            meta_manufacturer={
                "source_name": "Extraordinary commissioner for the Covid-19 emergency",
                "source_url": self.source_url,
            },
        )


def main():
    Italy().export()
