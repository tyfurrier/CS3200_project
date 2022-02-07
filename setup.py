import setuptools
from setuptools import setup

NAME = 'atscale'
DESCRIPTION = 'A package containing AI-link from AtScale and its accompanying tools, To-Do\'s: require LICENSE, ' \
                  'add README'
URL = 'https://github.com/tyfurrier/packaged_api'
AUTHOR = 'AtScale'
REQUIRES_PYTHON = '>=3.8.0'

REQUIRED = [
        'pandas>=1.3.5',
        'requests>=2.27.1',
        'colorama>=0.4.4',
        ]

GBQ_REQUIRED = ['sqlalchemy>=1.4.29', 'pybigquery>=0.10.2', 'pandas_gbq>=0.16.0']
DATABRICKS_REQUIRED = ['sqlalchemy>=1.4.29']
IRIS_REQUIRED = ['pyodbc>=4.0.32']
REDSHIFT_REQUIRED = ['sqlalchemy>=1.4.29', 'sqlalchemy-redshift>=0.8.9']
SNOWFLAKE_REQUIRED = ['sqlalchemy>=1.4.29', 'snowflake-sqlalchemy>=1.3.3']
SYNAPSE_REQUIRED = ['pyodbc>=4.0.32']
ATSPARK_REQUIRED = ['pyspark>=3.1.2']
DEV_REQUIRED = GBQ_REQUIRED + DATABRICKS_REQUIRED + IRIS_REQUIRED + REDSHIFT_REQUIRED \
               + SNOWFLAKE_REQUIRED + SYNAPSE_REQUIRED + ATSPARK_REQUIRED + ['IPython']
EXTRAS_REQUIRE = {
            'dev': DEV_REQUIRED,
            'gbq': GBQ_REQUIRED,
            'databricks': DATABRICKS_REQUIRED,
            'iris': IRIS_REQUIRED,
            'redshift': REDSHIFT_REQUIRED,
            'snowflake': SNOWFLAKE_REQUIRED,
            'synapse': SYNAPSE_REQUIRED,
      }

setup(name='atscale',
      version='0.0.1',
      description='A package containing AI-link from AtScale and its '
                  'accompanying tools, To-Do\'s: require LICENSE, add README,',
      url=URL,
      author='AtScale',
      author_email='tyler.furrier@atscale.com',
      packages=setuptools.find_packages(),
      install_requires=REQUIRED,
      extras_require=EXTRAS_REQUIRE,
      include_package_data=True,
      classifiers=['Programming Language :: Python :: 3.9'],
      )









