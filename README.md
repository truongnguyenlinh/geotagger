<img src="assets/image-20211210180125153.png" align="left">  
<br/><br/><br/><br/>

## Table of Contents :triangular_flag_on_post:

- Overview
- Installation

## Overview

As Vancouver locals, we can fail to recognize how unique and special the city is. Vancouver is home to many vibrant and interesting areas that consist of various tourist attractions, amenities and more. As part of our project, we collected GPX data from across the Lower Mainland to give a tour of various Vancouver must-see landmarks, restaurants, and more. Those who are in and around the Lower Mainland can make informed decisions on the things they want to see and go to with this data.

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
