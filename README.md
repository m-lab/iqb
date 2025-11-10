# Internet Quality Barometer (IQB)

This repository contains the source for code the Internet Quality Barometer (IQB)
library, and related applications and notebooks.

IQB is an open-source project initiated by [Measurement Lab (M-Lab)](https://www.measurementlab.net/).

IQB is motivated by the need to redefine how we measure and understand Internet
performance to keep pace with evolving technological demands and user
expectations. IQB is a comprehensive framework for collecting data and
calculating a composite score, the “IQB Score”, which reflects
the quality of Internet experience. IQB takes a more holistic approach
than “speed tests” and evaluates Internet performance across various
use cases (web browsing, video streaming, online gaming, etc.),
each with its own specific network requirements (latency, throughput, etc.).

Read more about the IQB framework in:

- M-Lab's [blog post](https://www.measurementlab.net/blog/iqb/).

- The IQB framework [detailed report](
https://www.measurementlab.net/publications/IQB_report_2025.pdf) and
[executive summary](
https://www.measurementlab.net/publications/IQB_executive_summary_2025.pdf).

- The IQB [poster](https://arxiv.org/pdf/2509.19034) at ACM IMC 2025.

## Repository Architecture

### **`data/`**

Sample datasets used in the IQB app prototype and notebooks. 

### **`library/`**

The IQB library containing methods for calculating the IQB score and data collection.

### **`prototype/`**

A simple web application implemented using Python's [streamlit](https://streamlit.io/)
library for applying and parametrizing the IQB framework in different use cases.

### **`analysis/`**

Jupyter notebooks for exploratory data analysis, experimentation, and research.

## Development Ennvironment

We use [uv](https://astral.sh/uv) as a replacement for several Python repository
management tools such as `pip`, `poetry`, etc.

On Ubuntu, you can install `uv` as follows:

```bash
snap install astral-uv
```
