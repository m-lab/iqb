from pprint import pprint

from .iqb_formula_config import IQB_CONFIG


class IQB:
    def __init__(self, config=None, name=None):
        """
        Initialize a new instance of IQB.

        Parameters:
            config (str): The file with the configuration of the IQB formula parameters. If "None" (default), it gets the parameters from the IQB_CONFIG dict.
            name (str): [Optional] name for the IQB instance.
        """
        self.set_config(config)
        self.name = name

    def set_config(self, config):
        """Sets up configuration parameters. If "None" (default), it gets the parameters from the IQB_CONFIG dict."""
        if config is None:
            self.config = IQB_CONFIG
        else:
            """
            TODO: load config data from file (json, yaml, or other format) as a dict
            """
            raise NotImplementedError(
                "method for reading from configuration file other than the default not implemented"
            )

    def print_config(self):
        """
        Prints IQB formula weights and thresholds
        TEMP function for testing purposes.
        TODO: to be updated
        """
        print("### IQB formula weights and thresholds")
        pprint(IQB_CONFIG)
        print()

        print("### Use cases")
        for uc in IQB_CONFIG["use cases"]:
            print(f"\t{uc}")
        print()

        print("### Network requirements")
        for uc in IQB_CONFIG["use cases"]:
            for nr in IQB_CONFIG["use cases"][uc]["network requirements"]:
                print(f"\t{nr}")
            break
        print()

        print("### Weights & Thresholds")
        print("\tUse case\t \tNetwork requirement \tWeight \tThreshold min")
        for uc in IQB_CONFIG["use cases"]:
            for nr in IQB_CONFIG["use cases"][uc]["network requirements"]:
                nr_w = IQB_CONFIG["use cases"][uc]["network requirements"][nr]["w"]
                nr_th = IQB_CONFIG["use cases"][uc]["network requirements"][nr]["threshold min"]
                print(f"\t{uc:20} \t{nr:20} \t{nr_w} \t{nr_th}")
        print()

    def calculate_binary_requirement_score(self, network_requirement, value, threshold):
        """
        Calculates binary requirement score for the given network requirement, value (i.e., data), and threshold (of the net requirement).
        - If the requirement is **throughput**, then the score is 1 if the given value is **larger** than the given threshold, and otherwise 0.
        - If the requirement is **latency or packet loss**, then the score is 1 if the given value is **smaller** than the given threshold, and otherwise 0.
        """
        if network_requirement == "download throughput":
            return 1 if value > threshold else 0
        elif network_requirement == "upload throughput":
            return 1 if value > threshold else 0
        elif network_requirement == "latency":
            return 1 if value < threshold else 0
        elif network_requirement == "packet loss":
            return 1 if value < threshold else 0
        else:
            raise ValueError(
                f"The binary requirement score method is not implemented for the network_requirement: {network_requirement}"
            )

    def calculate_iqb_score(self, data=None, print_details=False):
        """Calculates IQB score based on given data."""

        """
        TODO: Implement else case and remove this default (if) option
        """
        if data is None:
            # TODO: TEMP data sample. To be updated by reading a file or variable or other resource.
            measurement_data = {
                "m-lab": {
                    "download throughput": 15,
                    "upload throughput": 20,
                    "latency": 75,
                    "packet loss": 0.007,
                }
            }
        else:
            raise NotImplementedError(
                "Method for calculating IQB score given a dataset is not implemented"
            )

        """
        TODO: TEMP function for testing purposes. To be updated/polished (and remove prints)
        """
        uc_Scores = []
        uc_Weights = []

        for uc in self.config["use cases"]:
            uc_w = self.config["use cases"][uc]["w"]

            nr_Scores = []
            nr_Weights = []
            for nr in self.config["use cases"][uc]["network requirements"]:
                nr_w = self.config["use cases"][uc]["network requirements"][nr]["w"]
                nr_th = self.config["use cases"][uc]["network requirements"][nr]["threshold min"]

                # TODO: TEMP method for calculating binary requirement scores. To be updated with weighted average of scores per dataset
                ds_S = []
                for ds in self.config["use cases"][uc]["network requirements"][nr]["datasets"]:
                    ds_w = self.config["use cases"][uc]["network requirements"][nr]["datasets"][ds][
                        "w"
                    ]
                    if ds_w > 0:
                        brs = self.calculate_binary_requirement_score(
                            nr, measurement_data[ds][nr], nr_th
                        )  # binary requirement score (dataset, network requirement)
                        ds_S.append(brs)
                        print(
                            f"Binary score: {uc},{nr},{ds},{nr_th},{measurement_data[ds][nr]}-->{brs}"
                        ) if print_details else None
                ras = sum(ds_S) / len(
                    ds_S
                )  # requirement agreement score (all datasets for this requirement)
                print(f"\t Agreement score: {uc},{nr}-->{ras}") if print_details else None
                #

                nr_Scores.append(ras * nr_w)
                nr_Weights.append(nr_w)

            ucs = sum(nr_Scores) / sum(
                nr_Weights
            )  # use case score (all requirements for this use case)
            print(
                f"\t\t Net requirement score: {nr_Scores},{nr_Weights}-->{ucs}\n"
            ) if print_details else None
            uc_Scores.append(ucs * uc_w)
            uc_Weights.append(uc_w)

        iqb_score = sum(uc_Scores) / sum(uc_Weights)
        print(
            f"\t\t\t IQB score: {uc_Scores},{uc_Weights}-->{iqb_score}"
        ) if print_details else None

        return iqb_score
