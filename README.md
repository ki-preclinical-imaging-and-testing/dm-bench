# dm-bench
Tools for data management benchmarking

## benchmark-basics.py
This script contains a diagnostic routine for testing
COPY/EXTRACT/COMPRESS/DELETE actions from one folder to another. It is designed
as a diagnostic for working with files via a local mountpoint and remains
protocol agnostic.

It also contains several data management functions that may support other use
cases. Current version requires manual update of parameters inside of
`benchmark-basics.py`. Furthermore, at runtime, if the mountpoint requires
super-user privileges, then you likely need to run with the following syntax:

   $ sudo env PATH="$PATH" python benchmark-basics.py 

This provides priveleges while retaining your user's Python path.

## `benchmark-basics-analysis.ipynb`
This iPython notebook provides some basic functions for opening the output of
`benchmark-basics.py` and analyzing it with an eye for speed and scaling.

