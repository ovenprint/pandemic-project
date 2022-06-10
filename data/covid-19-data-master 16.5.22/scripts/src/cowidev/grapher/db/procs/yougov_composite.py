from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherYougovCompUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="YouGov-Imperial COVID-19 Behavior Tracker, composite variables",
            source_name=(
                f"Imperial College London YouGov Covid 19 Behaviour Tracker Data Hub – Last updated {self.time_str}"
            ),
            zero_day="2020-01-21",
        )
