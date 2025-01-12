# ETL for Vinyl Shop

## Overview
This project implements an **ETL (Extract, Transform, Load)** process designed to handle data for a vinyl shop. The system collects data from various sources, transforms it to meet the shop's analytical and operational requirements, and loads it into a target database for further use.

The project is built with Python and is structured to allow for scalability, maintainability, and ease of integration with modern orchestration tools like **Apache Airflow**.

---

## Features
- **Extract:** 
  - Import data from multiple sources, such as CSV files, APIs, or databases.
- **Transform:** 
  - Validate, clean, and standardize the data to meet business requirements.
  - Handle duplicate records and ensure data consistency.
- **Load:** 
  - Insert the processed data into a database or data warehouse.

---

## Requirements
To run the project, you need:
- Python 3.8+
- Required Python libraries (listed in `requirements.txt`):
  - `pandas`
  - `sqlalchemy`
  - `psycopg2` (for PostgreSQL)
  - `pytest` (for testing)

---

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/WojciechZaczek/ETL-for-Vinyl-Shop.git
   cd ETL-for-Vinyl-Shop
   ```

2. Create a virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage
### Manual Execution
1. Place your input files (e.g., CSV) in the designated input directory.
2. Run the script:
   ```bash
   python main.py
   ```

### Airflow Integration (Planned)
In the next phase, this project will integrate with **Apache Airflow** for full automation and scheduling of the ETL pipeline.

---

## Folder Structure
```
ETL-for-Vinyl-Shop/
├── data/
│   ├── input/          # Raw data files
│   ├── output/         # Processed data
├── src/
│   ├── extract.py      # Data extraction logic
│   ├── transform.py    # Data transformation logic
│   ├── load.py         # Data loading logic
│   ├── utils.py        # Helper functions
├── tests/              # Unit and integration tests
├── requirements.txt    # Python dependencies
├── main.py             # Entry point of the ETL pipeline
└── README.md           # Project documentation
```

---

## Contributing
Contributions are welcome! Feel free to submit issues or pull requests to enhance the project.

---

## License
This project is licensed under the [MIT License](LICENSE).

---

## Contact
Created by **Wojciech Zaczek**  
For inquiries, reach out via [GitHub Issues](https://github.com/WojciechZaczek/ETL-for-Vinyl-Shop/issues).
