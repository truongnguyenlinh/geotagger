<img src="assets/image-20211210180125153.png" align="left">  
<br/><br/><br/><br/>

## Table of Contents :triangular_flag_on_post:

- Installation
- Usage

## Installation

1. Install `pip` and `python3.8` or greater

2. Create virtual environment

   ```bash
   $ python3 -m venv venv
   ```

3. Activate virtual environment

   #### Linux or MacOS

   ```bash
   $ source venv/bin/activate
   ```

   #### Windows

   ```bash
   $ venv/scripts/activate
   ```

4. Install Dependencies

   ```bash
   $ pip3 install -r requirements.txt
   ```

## Usage

To run the application, complete the following steps:

1. Running pyspark via the Terminal:

   ```bash
   $ spark-submit track.py osm
   ```

2. Running the code via a Jupyter Notebook:

    ```bash
    $ jupyter trust osm-analysis.ipynb
    $ jupyter notebook osm-analysis.ipynb
    ```
