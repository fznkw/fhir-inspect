fhir-inspect
============
Python script which fetches, calculates and displays meta information of a FHIR server.

Requirements
------------

The Python modules [fhirclient] and [rich] are needed:

    pip install fhirclient rich
    
Usage
-----

```
fhir_inspect.py [-h] (-l | -r RESOURCE | -s) [--items] [--level N] [--limit N] [--novalidation] [--zero] [--version] URL

positional arguments:
  URL             URL of the FHIR server

optional arguments:
  -h, --help      show this help message and exit
  -l, --list      list and count all resources
  -r RESOURCE     inspect given resource: output a tree view with resource items and their count
  -s              list structure definitions
  --items         when inspecting a resource, also inspect resources item values
  --level N       maximum level up to which the hierarchy is displayed in the tree view (default: 10)
  --limit N       limit number of FHIR resources to receive
  --novalidation  turn validation off (default: on)
  --zero          when listing all resources, output resources with count zero (will be omitted otherwise)
  --version       show program's version number and exit
```
    
Example use
-----------

List and count all resources on the server:

    python fhir_inspect.py -l https://server.fire.ly

Inspect resource "Patient" and it's values. Print tree view with resource items, their count, item values and item value count:
    
    python fhir_inspect.py -r Patient --items https://server.fire.ly
    
[fhirclient]: https://github.com/smart-on-fhir/client-py
[rich]: https://github.com/Textualize/rich
