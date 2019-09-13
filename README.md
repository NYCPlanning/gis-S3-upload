# S3 Bucket Upload

*******************************

This script is used for migrating updated internal DCP data set zips to one of Data Engineering's S3 buckets for various DE processing. The data sets which are currently uploaded by this process are:


* <b>MapPLUTO</b>
* <b>Transit Zones</b>
* <b>Upland Waterfront Area</b>
* <b>Coastal Zone Boundary (CZB)</b>
* <b>Waterfront Access Plan (WAP)</b>
* <b>Mandatory Inclusionary Housing (MIH)</b> 
* <b>Environmental Designations (E-Designations)</b>
* <b>FRESH Food Stores Zoning Boundaries (FRESH)</b>
* <b>Inclusionary Housing Designated Areas (IDHA)</b>
* <b>Lower Density Growth Management Areas (LDGMA)</b>
* <b>Designated Areas in Manufacturing Districts (Appendix J)</b>
* LPC data sets (<b>Historic Districts</b>, <b>Individual Landmarks</b>, <b>Scenic Landmarks</b>)
* Digital Tax Map/DTM data sets (<b>Block Centroids</b>, <b>Tax Lot Polygons</b>, <b>Shoreline Polygons</b>, <b>Condos</b>)
* Zoning data sets (<b>Commercial Overlays</b>, <b>Limited Height Districts</b>, <b>Special Purpose Districts</b>, <b>Special Purpose Sub-districts</b>, <b>Zoning Districts</b>, <b>Zoning Map Amendments</b>, <b>Sidewalk Cafe Regulations</b>)


### Prerequisites

An installation of Python 2 with the following packages is required. A version of Python with the default ArcPy installation that comes with ArcGIS Desktop is required in order to utilize Metadata functionality that is currently not available in the default ArcPy package that comes with ArcGIS Pro (Python 3) installation or the 64-bit Arcpy package that comes with 64-bit Background Geoprocessing.

##### S3\_Upload\_Script.py

```
boto3, os, datetime, arcpy, shutil, zipfile, ConfigParser, traceback, time, sys, xml.etree.ElementTree as ET
```

### Instructions for running

##### S3\_Upload\_Script.py

1. Open the script in any integrated development environment (PyCharm is suggested)

2. Ensure that your IDE is set to be utilizing a version of Python 2 with the arcpy module installed, as well as the above listed required python packages.

3. Ensure that the configuration ini file is up-to-date with path variables and S3 credentials. If any paths or credentials have changed since the time of this writing, those changes must be reflected in the Config.ini file.

4. Run the script. It will generate a temporary directory called s3_upload on your local drive. The script will then populate the temporary directory by pulling the above listed data sets from various locations of DCP's internal network drives and ArcSDE. 

5. The data sets are then compressed and uploaded (with public read access) to the appropriate S3 bucket for Data Engineering team to utilize. 