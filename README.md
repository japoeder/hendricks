# Hendricks

### Introduction

This is the core data loading service for the following:

1. Simple load that drops existing records in raw collection, and reloads data over the specified window
   * May need to load year by year if you aren't sure how far back the data goes.
2. QC checks missing minutes over a specified window, and attempts to reload
3. Stream load reads live data into the raw price collection

### Service

1. Restart the service if necessary with:
   * qt_restart_hl
2. Sample request:
   * qt_hendricks_load -t AAPL -s "2024-11-01T00:00:00" -e "2024-11-15T23:59:00"
   * Note that this calls a zsh alias that calls a shell script.  See the

### Usage Details:

```commandline
simple load: qt_hendricks_load via the terminal
  
optional "parameters "arguments:
  -t    Ticker symbol (required)
  -f    File (optional)
  -s    From date (default: 2024-10-03T09:30:00-04:00)
  -e    To date (default: 2024-10-04T00:59:32-04:00)
  -c    Collection name (default: rawPriceColl)
  -b    Batch size (default: 50000)
  -h    Show this help message
```

## Python Packaging

This code is packaged as a python module, with the structure outlined in the section below.

### Project Layout

* [pid_0001_collectability_model_v1](collectability_model): The parent or "root" folder containing all of these files. Can technically have any name.
  * [README.md](README.md):
    The guide you're reading.
  * [`__init__.py`](lab1/__init__.py)
    For package / module structure.
  * [`.`](lab1/__init__.py)gitignore
    Version control doc
  * .pre-commit-config.yaml
    Pre-commit hooks for formatting and linting
  * .pylintrc
    Linter parameter file.
  * [`lint.py`](lab1/__init__.py)
    Linter driver.
  * [`p`](lab1/__init__.py)yproject.toml
    Black exception logic.
  * [`r`](lab1/__init__.py)eq.txt
    Required libraries for model.
  * [collectability_model](.): This is the *module* in our *package*.
    * [`html`](lab1/__init__.py)
      Directory that holds html output
    * [`t`](lab1/__init__.py)emplates
      Directory that holds templates for base and enhanced model scenarios and tuning.
    * [`u`](lab1/__init__.py)tilities
      * [`m`](lab1/__init__.py)ake_json.py
        Shell for a program to create a json payload if data comes in another format
      * [`m`](lab1/__init__.py)l_utils.py
        Cos similarity model
      * open_html.py
        Opens html output if requested
    * [`__init__.py`](lab1/__init__.py)
      Expose what functions, variables, classes, etc when scripts import this module.
    * [`__main__.py`](lab1/__main__.py)
      This file is the entrypoint to your program when ran as a program.
    * `collectability_model.py`
      Driver program for the module
    * `build_report_template.py`
      Customizes html template for score report
    * `load_scenario.py`
      Method to read in data
    * `load_tuning.py`
      Helper method to read in tuning parameters
    * `proc_wams.py`
      Create weights and measures used in methodology
    * `score_report.py`
      Generate the final score report using customized template
    * `stage_i_scoring.py`
      Stage I scoring to assess fraud
    * `stage_ii_scoring.py`
      Stage II scoring to get modeled collectability score
    * `stage_iii_planning_calcs.py`
      Run final calculations for payment / settlement planning
    * [`t`](lab1/__init__.py)uning.json
      Default tuning sensitivities for attribute weights
    * `validate_input_files.py`
      Validates the json inputs (raw data and tuning)
    * `utilities/make_json.py`
      Use to format raw data if not in json format
    * `utilities/open_html.py`
      Opens the system browser and displays report
