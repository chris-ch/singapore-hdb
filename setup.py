from setuptools import setup, find_packages
import py2exe
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='singapore-hdb',
    version='0.1',
    packages=find_packages('src'),
    package_dir = {'':'src'},   # for distutils
    scripts=['scripts/hdbretrieve.py'],
    url='',
    license='',
    author='Christophe Alexandre',
    author_email='ch.alexandre@bluewin.ch',
    description='Extracting building data from HDB website',
    long_description=read('README.md'),
    # for py2exe
    console=['scripts/hdbretrieve.py'],
    entry_points={
        'console_scripts': [
            'hdbretrieve = hdb:main',
        ],
    },
    install_requires = [
        'beautifulsoup4>=4.4.1',
        'retrying>=1.3.3',
        'pandas>=0.18.1',
        'requests>=2.10.0',
        'lxml>=3.6.0',
    ],
)
