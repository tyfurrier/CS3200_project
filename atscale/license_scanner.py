import pandas
import pkg_resources
from IPython.display import display
REQUIRED = [
        'pandas>=1.3.5',
        'requests>=2.27.1',
        'pyspark>=3.1.2',
        'colorama>=0.4.4',
        ]

GBQ_REQUIRED = ['sqlalchemy>=1.4.29', 'pybigquery>=0.10.2']
DATABRICKS_REQUIRED = ['sqlalchemy>=1.4.29']
IRIS_REQUIRED = ['pyodbc>=4.0.0']
REDSHIFT_REQUIRED = ['sqlalchemy>=1.4.29', 'sqlalchemy-redshift>=0.8.9']
SNOWFLAKE_REQUIRED = ['sqlalchemy>=1.4.29', 'snowflake-sqlalchemy>=1.3.3']
SYNAPSE_REQUIRED = ['pyodbc>=4.0.0']
#ATSPARK_REQUIRED todo
DEV_REQUIRED = REQUIRED + GBQ_REQUIRED + DATABRICKS_REQUIRED + IRIS_REQUIRED \
               + REDSHIFT_REQUIRED + SNOWFLAKE_REQUIRED + SYNAPSE_REQUIRED # + ATSPARK_REQUIRED

DEV_SHORTENED = []

INCLUDE_COPYRIGHT = ['Original copyright must be retained']
INCLUDE_LICENSE = ['Including the full text of license in modified software']
STATE_CHANGES = ['state changes in text file']
INCLUDE_NOTICE = ['Must include library\'s NOTICE file with attribution notes \n May append to this NOTICE file']
UNKNOWN_LICENSE = []
UNIDENTIFIED_LICENSE = []
license_necessities = {'MIT': [INCLUDE_LICENSE, INCLUDE_COPYRIGHT],
                       'BSD': [['free reign']],
                       'BSD-3-Clause': [INCLUDE_LICENSE, INCLUDE_COPYRIGHT],
                       'UNKNOWN': [UNKNOWN_LICENSE],
                       'Apache 2.0': [INCLUDE_NOTICE, INCLUDE_COPYRIGHT, INCLUDE_LICENSE,
                                      STATE_CHANGES],
                       'Apache License, Version 2.0': [INCLUDE_NOTICE, INCLUDE_COPYRIGHT, INCLUDE_LICENSE,
                                      STATE_CHANGES],
                       'http://www.apache.org/licenses/LICENSE-2.0': [INCLUDE_NOTICE, INCLUDE_COPYRIGHT,
                                                                      INCLUDE_LICENSE, STATE_CHANGES],
                       'Apache': [INCLUDE_LICENSE, INCLUDE_COPYRIGHT]}

for name in DEV_REQUIRED:
    cut = name.split('>')
    if len(cut) > 1:
        DEV_SHORTENED.append(cut[0])
    else:
        cut = name.split(' as')
        if len(cut) > 1:
            DEV_SHORTENED.append(cut[0])
        else:
            cut = name.split('.')
            if len(cut) > 1:
                DEV_SHORTENED.append(cut[0])
            else:
                DEV_SHORTENED.append(name)
print(DEV_SHORTENED)

Apache2Requirements =\
    """must include the following in their copy of the code, whether they have modified it or not:
                    The original copyright notice
                    A copy of the license itself
                    If applicable, a statement of any significant changes made to the original code
                    A copy of the NOTICE file with attribution notes (if the original library has one)"""

license_permission = {'Apache 2.0': 'COMMERCIAL USE CHECK',
                      'MIT': 'COMMERCIAL USE CHECK',
                      'BSD': 'COMMERCIAL USE',



                      }
license_type = {}

def get_pkg_license(pkg):
    try:
        lines = pkg.get_metadata_lines('METADATA')
    except:
        lines = pkg.get_metadata_lines('PKG-INFO')

    for line in lines:
        if line.startswith('License:'):
            return line[9:]
    return '(Licence not found)'

def print_packages_and_licenses():
    packages = []
    licenses = []
    for pkg in sorted(pkg_resources.working_set, key=lambda x: str(x).lower()):
        if str(pkg).split(' ')[0] in DEV_SHORTENED:
            packages.append(str(pkg).split(' ')[0])
            licenses.append(get_pkg_license(pkg))
            if license_necessities[get_pkg_license(pkg)]:
                for necessity in license_necessities[get_pkg_license(pkg)]:
                    necessity.append(str(pkg).split(' ')[0])
            else:
                UNIDENTIFIED_LICENSE.append(str(pkg))
    dict = {'Package': packages,
           'License': licenses}
    not_found = [x for x in DEV_SHORTENED if x not in packages]
    print(not_found)
    print_legalities()
    df = pandas.DataFrame(dict)
    display(df)

def print_legalities():
    print(f'UNIDENTIFIED_LICENSE:{UNIDENTIFIED_LICENSE}')
    print(f'INCLUDE_LICENSE: {INCLUDE_LICENSE}')
    print(f'INCLUDE_COPYRIGHT: {INCLUDE_COPYRIGHT}')
    print(f'INCLUDE_NOTICE: {INCLUDE_NOTICE}')
    print(f'STATE_CHANGES: {STATE_CHANGES}')
    print(f'UNKNOWN_LICENSES: {UNKNOWN_LICENSE}')
def print_packages_and_licenses_V2():
    packages = []
    licenses = []
    for pkg in sorted(pkg_resources.working_set, key=lambda x: str(x).lower()):
        if str(pkg)[0] == 'p':
            packages.append(str(pkg))
            licenses.append(get_pkg_license(pkg))
    dict = {'Package': packages,
           'License': licenses}
    not_found = [x for x in DEV_SHORTENED if x not in packages]
    print(not_found)
    df = pandas.DataFrame(dict)
    display(df)


if __name__ == "__main__":
    print_packages_and_licenses_V2()
    print_packages_and_licenses()