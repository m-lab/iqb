"""Module that implements calculating IQB scores."""

import copy
from pprint import pprint

from .config import IQB_CONFIG


class IQBCalculator:
    """Component that calculates IQB scores."""

    def __init__(self, config=None, name=None):
        """
        Initialize a new instance of IQBCalculator.

        Parameters:
            config (dict | str): If "None" (default), use IQB_CONFIG. A dict is used
                as in-memory configuration. A string is currently treated as a
                configuration file path and is not implemented.
            name (str): [Optional] name for the IQBCalculator instance.
        """
        self.set_config(config)
        self.name = name

    def set_config(self, config):
        """Set up configuration parameters."""
        if config is None:
            self.config = IQB_CONFIG
        elif isinstance(config, dict):
            # Keep calculator behavior isolated from external mutations.
            self.config = copy.deepcopy(config)
        elif isinstance(config, str):
            # TODO: load config data from file (json, yaml, or other format) as a dict
            raise NotImplementedError(
                "method for reading from configuration file other than the default not implemented"
            )
        else:
            raise TypeError(
                f"config must be None, dict, or str, got: {type(config).__name__}"
            )

    def print_config(self):
        """
        Prints IQB formula weights and thresholds
        TEMP function for testing purposes.
        """
        # TODO: to be updated
        print("### IQB formula weights and thresholds")
        pprint(self.config)
        print()

        print("### Use cases")
        for uc in self.config["use cases"]:
            print(f"\t{uc}")
        print()

        print("### Network requirements")
        for uc in self.config["use cases"]:
            for nr in self.config["use cases"][uc]["network requirements"]:
                print(f"\t{nr}")
            break
        print()

        print("### Weights & Thresholds")
        print("\tUse case\t \tNetwork requirement \tWeight \tThreshold min")
        for uc in self.config["use cases"]:
            for nr in self.config["use cases"][uc]["network requirements"]:
                nr_w = self.config["use cases"][uc]["network requirements"][nr]["w"]
                nr_th = self.config["use cases"][uc]["network requirements"][nr]["threshold min"]
                print(f"\t{uc:20} \t{nr:20} \t{nr_w} \t{nr_th}")
        print()

    def calculate_binary_requirement_score(self, network_requirement, value, threshold):
        """
        Calculates binary requirement score for the given network requirement, value (i.e., data), and threshold (of the net requirement).
        - If the requirement is **throughput**, then the score is 1 if the given value is **larger** than the given threshold, and otherwise 0.
        - If the requirement is **latency or packet loss**, then the score is 1 if the given value is **smaller** than the given threshold, and otherwise 0.
        """
        if network_requirement == "download_throughput_mbps":
            return 1 if value > threshold else 0
        elif network_requirement == "upload_throughput_mbps":
            return 1 if value > threshold else 0
        elif network_requirement == "latency_ms":
            return 1 if value < threshold else 0
        elif network_requirement == "packet_loss":
            return 1 if value < threshold else 0
        else:
            raise ValueError(
                f"The binary requirement score method is not implemented for the network_requirement: {network_requirement}"
            )

    def calculate_iqb_score(self, data=None, print_details=False):
        """Calculates IQB score based on given data."""

        # TODO(bassosimone): remove the default data sample in a subsequent interation.
        sample_data = {
            "m-lab": {
                "download_throughput_mbps": 15,
                "upload_throughput_mbps": 20,
                "latency_ms": 75,
                "packet_loss": 0.007,
            }
        }
        data = sample_data if data is None else data

        # TODO(bassosimone): remove printing from the current function and instead
        # add more tests to gain better confidence about it being WAI
        doprint = print if print_details else lambda *args, **kwargs: None

        uc_scores = []
        uc_weights = []

        for uc in self.config["use cases"]:
            uc_w = self.config["use cases"][uc]["w"]

            nr_scores = []
            nr_weights = []
            for nr in self.config["use cases"][uc]["network requirements"]:
                nr_w = self.config["use cases"][uc]["network requirements"][nr]["w"]
                nr_th = self.config["use cases"][uc]["network requirements"][nr]["threshold min"]

                # TODO: TEMP method for calculating binary requirement scores. To be
                # updated with weighted average of scores per dataset.
                ds_s = []
                for ds in self.config["use cases"][uc]["network requirements"][nr]["datasets"]:
                    ds_w = self.config["use cases"][uc]["network requirements"][nr]["datasets"][ds][
                        "w"
                    ]
                    if ds_w > 0:
                        # binary requirement score (dataset, network requirement)
                        brs = self.calculate_binary_requirement_score(nr, data[ds][nr], nr_th)
                        ds_s.append(brs)
                        doprint(f"Binary score: {uc},{nr},{ds},{nr_th},{data[ds][nr]}-->{brs}")

                # requirement agreement score (all datasets for this requirement)
                ras = sum(ds_s) / len(ds_s)
                doprint(f"\t Agreement score: {uc},{nr}-->{ras}")

                nr_scores.append(ras * nr_w)
                nr_weights.append(nr_w)

            # use case score (all requirements for this use case)
            ucs = sum(nr_scores) / sum(nr_weights)
            doprint(f"\t\t Net requirement score: {nr_scores},{nr_weights}-->{ucs}\n")
            uc_scores.append(ucs * uc_w)
            uc_weights.append(uc_w)

        iqb_score = sum(uc_scores) / sum(uc_weights)
        doprint(f"\t\t\t IQB score: {uc_scores},{uc_weights}-->{iqb_score}")
        return iqb_score
