from cowidev.vax.utils.base import CountryVaxBase
import pandas as pd

from cowidev.utils.clean import clean_date_series
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.utils import make_monotonic


class Malta(CountryVaxBase):
    location: str = "Malta"
    source_url: str = (
        "https://github.com/COVID19-Malta/COVID19-Cases/raw/master/COVID-19%20Malta%20-%20Vaccination%20Data.csv"
    )
    source_url_ref: str = "https://github.com/COVID19-Malta/COVID19-Cases"
    columns_rename: dict = {
        "Date of Vaccination": "date",
        "Total Vaccination Doses": "total_vaccinations",
        "Fully vaccinated (2 of 2 or 1 of 1)": "people_fully_vaccinated",
        "Received one dose (1 of 2 or 1 of 1)": "people_vaccinated",
        "Total Booster doses": "total_boosters",
        "Total 2nd Booster doses": "total_boosters_2",
    }

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_url)
        # Temporal fix
        df.columns = [col.replace("\ufeff", "") for col in df.columns]
        check_known_columns(
            df,
            [
                "Date of Vaccination",
                "Total Vaccination Doses",
                "Fully vaccinated (2 of 2 or 1 of 1)",
                "Received one dose (1 of 2 or 1 of 1)",
                "Total Booster doses",
                "Total 2nd Booster doses",
            ],
        )
        return df

    def pipe_check_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns_wrong = set(df.columns).difference(self.columns_rename)
        if columns_wrong:
            raise ValueError(f"Invalid column name(s): {columns_wrong}")
        return df

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def pipe_correct_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[
            (df.people_fully_vaccinated == 0) | df.people_fully_vaccinated.isnull(),
            "people_vaccinated",
        ] = df.total_vaccinations
        df = df.assign(total_boosters=df.total_boosters + df.total_boosters_2)
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date, "%d/%m/%Y"))

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine_name(date: str) -> str:
            # See timeline in:
            if date < "2021-02-03":
                return "Pfizer/BioNTech"
            if "2021-02-03" <= date < "2021-02-10":
                return "Moderna, Pfizer/BioNTech"
            elif "2021-02-10" <= date < "2021-05-06":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            elif "2021-05-06" <= date:
                return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
        )

    def pipe_exclude_data_points(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            (df.people_vaccinated >= df.people_fully_vaccinated)
            | (df.people_vaccinated.isna())
            | (df.people_fully_vaccinated.isna())
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_check_columns)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_correct_data)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_exclude_data_points)
            .pipe(make_monotonic)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, valid_cols_only=True)


def main():
    Malta().export()
