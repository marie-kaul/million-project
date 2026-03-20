### Overview
This directory contains input files for example scripts in GentzkowLabTemplate.

See the Examples section of the [template instructions](https://github.com/gentzkow/GentzkowLabTemplate/wiki#examples) for the general procedure for using example scripts.

### Source

* `mpg.csv` is the raw data file for the examples. `mpg.csv` was exported from the `mpg` dataset shipped with R's ggplot2 package. See [here](https://rpubs.com/shailesh/mpg-exploration) for more detail. Exported manually by Matthew Gentzkow 12/15/2023. This file should be placed in `$EXTERNAL_PATHS[0]/0_raw/` (external data directory).

* `figure_city.jpg`, `figure_hwy.jpg`, and `table_reg.tex` were produced by the script `analyzed_data.do` in `/examples/stata/`. These files can be copied from `inputs_for_examples` to `4_paper/input/` to allow the slides and paper example scripts to run without needing to run any data building or analysis scripts. 

