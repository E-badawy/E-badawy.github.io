# Global Spatial Analysis of Childhood Stunting using R and GIS

## Overview

This project presents a reproducible spatial analysis of childhood stunting prevalence across **162 countries** using **R**, **Quarto**, and **Geographic Information Systems (GIS)**. The analysis integrates descriptive statistics, exploratory data analysis, and choropleth mapping to communicate the global distribution of childhood stunting through a transparent and reproducible analytical workflow.

Rather than focusing solely on data visualisation, the project demonstrates how statistical summaries and spatial analysis can be combined to support evidence-based public health reporting.

---

## Problem Statement

Childhood stunting remains one of the most important indicators of chronic undernutrition worldwide. Although country-level prevalence estimates are widely available, numerical tables alone make it difficult to understand geographical patterns and identify regions experiencing a greater nutritional burden.

This project addresses that challenge by transforming country-level prevalence data into interpretable statistical summaries and publication-quality spatial visualisations.

---

## Objectives

The objectives of this project were to:

* Describe the distribution of childhood stunting across countries.
* Perform data quality assessment and descriptive statistical analysis.
* Identify countries with the highest and lowest prevalence.
* Visualise the geographical distribution of childhood stunting using GIS.
* Demonstrate a fully reproducible analytical workflow using R and Quarto.

---

## Dataset

* **Observations:** 162 countries
* **Variables:**

  * ISO 3166-1 Alpha-3 Country Code
  * Country Name
  * Childhood Stunting Prevalence (%)

> **Note:** This project uses a secondary dataset obtained for educational and portfolio purposes. If the original source is confirmed (e.g. UNICEF), the repository will be updated accordingly.

---

## Methodology

The analysis followed a reproducible workflow consisting of:

1. Data import and cleaning
2. Data quality assessment
3. Descriptive statistical analysis
4. Exploratory data analysis
5. Identification of high- and low-prevalence countries
6. Spatial data integration using ISO country codes
7. Choropleth mapping
8. Interpretation and reporting using Quarto

---

## Key Findings

* Childhood stunting prevalence ranged from **1.0%** to **55.3%**.
* The average prevalence across the analysed countries was **16.2%**.
* Burundi recorded the highest prevalence, while Poland recorded the lowest.
* The distribution of stunting prevalence was positively skewed, indicating that relatively few countries experience exceptionally high prevalence.
* Spatial analysis revealed higher prevalence across several countries in West, Central, Eastern and Southern Africa, while comparatively lower prevalence was observed across much of Europe, North America and Australia.

---

## Technologies Used

* R
* Quarto
* tidyverse
* sf
* ggplot2
* readxl
* viridis
* janitor

---

## Repository Structure

```text
Global-Stunting-GIS/

├── data/
├── output/
├── Global-Stunting.qmd
├── Global-Stunting.html
├── README.md
├── .gitignore
└── Global-Stunting-GIS.Rproj
```

---

## Reproducing the Analysis

1. Clone the repository.
2. Open the `.Rproj` file in RStudio.
3. Install the required R packages.
4. Render `Global-Stunting.qmd`.

---

## Future Improvements

Potential extensions include:

* Temporal analysis using multiple years of data.
* Spatial autocorrelation analysis (Moran's I).
* Integration with socioeconomic and health indicators.
* Predictive modelling of childhood stunting.
* Sub-national spatial analysis where regional data are available.

---

## Author

**Badawi Aminu Muhammed**

Data Scientist specialising in Artificial Intelligence, Machine Learning, and Monitoring & Evaluation (M&E) Analytics, with a strong interest in spatial epidemiology, public health analytics, and evidence-based decision-making.
