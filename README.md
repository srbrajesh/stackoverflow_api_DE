# Stack Overflow Data Processor
This project provides a set of functions to fetch, process, and store Stack Overflow data using the Stack Exchange API and Apache Spark.

## Requirements
- Python 3.x
- requests library
- PySpark

## Project Structure
The project is structured as follows:
- `src/`: Contains the source code files.
    - `main.py`: Entry point of the application or sample usage code.
    - `data_processor/`: Package for Stack Overflow data processing.
        - `__init__.py`: Package initialization file.
        - `stackoverflow_data_processor.py`: Contains the `StackOverflowDataProcessor` class.

- `tests/`: Contains unit tests for the code in the `src/` folder.
    - `test_stackoverflow_data_processor.py`: Unit tests for the `StackOverflowDataProcessor` class.

## Running Instructions
**This code has been tested on Google Colab.**
- Create Virtual Environment: ```python3 -m venv env```
- Install requirements.txt: ```pip3 install -r requirements.txt```
- Run the code: ```python3 src/main.py```
- Run testcases: ```python3 test_stackoverflow_data_processor.py```
