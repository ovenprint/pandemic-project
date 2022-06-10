from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherVariantsUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - Variants",
            source_name=f"GISAID, via CoVariants.org – Last updated {self.time_str}",
            zero_day="2020-01-21",
            unit="%",
            unit_short="%",
        )


class GrapherSequencingUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - Sequencing",
            source_name=f"GISAID, via CoVariants.org – Last updated {self.time_str}",
            zero_day="2020-01-21",
        )
