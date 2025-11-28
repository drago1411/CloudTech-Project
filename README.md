Olympic Data Engineering & Analytics Project (PySpark + Python Dashboard)
Author: Hareeshwar & Lokeshwar
Dataset: 120 Years of Olympic History — Athletes & Results
Tools: PySpark, Python, Pandas, Seaborn, Matplotlib, JupyterLab

Project Overview:
This project performs end-to-end data engineering and analytics on the 120 Years of Olympic History dataset using PySpark for the ETL pipeline
and Python (Pandas, Seaborn, Matplotlib) for dashboards and insights.
The goal of the project is to:
-Clean and standardize raw Olympic athlete and NOC datasets
-Build analytical data outputs using PySpark transformations
Generate insights such as:
-Total medal achievements by country
-Gender participation over time
-Dominant sport by nation
-Produce visualization figures for final reporting and dashboarding
-Provide reproducible scripts and a clear project structure

clouddataset-olympic-analysis
│
├── data/                          # Raw CSV files (NOT in GitHub)
│   ├── athlete_events.csv
│   └── noc_regions.csv
│
├── output/                        
│   ├── medal_tally_all_years.csv
│   ├── athlete_career_summary.csv
│   ├── event_participation_by_year.csv
│   ├── top_countries_all_time.csv
│   ├── gender_participation_trends.csv
│   ├── dominant_sport_per_country.csv
│   └── figures/                   # All charts for the report
│       ├── top15_countries_total_medals.png
│       ├── medals_over_time_top6.png
│       ├── gender_participation_trends.png
│       ├── top_sports_by_athletes.png
│       └── dominant_sport_top20.png
│
├── etl_spark.py                   # Full PySpark ETL pipeline
├── small_etl.py                   # Lightweight Spark script (Windows-friendly)
├── generate_extra.py              # Extra aggregations (gender+sport dominance)
│
├── notebooks/
│   └── dashboard_analysis.ipynb   # Visualization notebook
│
├── requirements.txt               # Python dependencies
├── .gitignore
└── README.md                      # This file

Dataset from Kaggle:
https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results

ETL Pipeline (PySpark):

Step 1 — Load Raw CSVs
PySpark loads:
athlete_events.csv
noc_regions.csv

Step 2 — Clean Athlete Data
Standardize column names
Remove null entries
Normalize medal names (Gold/Silver/Bronze → 1 else 0)

Step 3 — Join NOC Regions
Merge athlete data with region names from NOC mapping.

Step 4 — Compute Metrics
Generate outputs

Technologies Used:

1.PySpark → ETL, large dataset processing
2.Pandas → Post-ETL analysis
3.Matplotlib & Seaborn → Visualizations
4.JupyterLab → Analytical exploration
5.GitHub → Version control and project distribution
6.Python virtual environments → Environment isolation

Project Outcome:

- Understanding of big-data ETL workflows
- Ability to run PySpark on Windows
- Building analytical datasets
- Creating dashboards
- Clean coding and documentation
- Use of Git/GitHub for project submission

Contact:
For any questions:
harishganapathy1411@gmail.com
Hareeshwar
