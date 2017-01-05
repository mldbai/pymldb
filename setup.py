"""setuptools based setup module for pymldb.

based on:
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Defines __version__
exec(open('pymldb/version.py').read())

setup(
    name='pymldb',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    # https://www.python.org/dev/peps/pep-0440/#public-version-identifiers
    version=__version__,

    description='Python interface to MLDB',
    long_description=long_description,

    url='https://github.com/datacratic/pymldb',
    author='Datacratic',
    author_email='mldb@datacratic.com',
    license='BSD',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',

        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',

        'Intended Audience :: Developers',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],

    keywords='machine learning',

    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        'numpy',
        'pandas',
        'pygments',
        'requests[security]>=2.6',
    ],
)
