import re
from setuptools import setup, find_packages
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def get_version():
    with open(os.path.sep.join(['src', 'hdb', '__init__.py']), 'r') as fd:
        version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                            fd.read(), re.MULTILINE).group(1)

    if not version:
        raise RuntimeError('Cannot find version information')

    return version

setup(
    name='singapore-hdb',
    version=get_version(),
    packages=find_packages('src'),
    package_dir={'': 'src'},   # for distutils
    scripts=['scripts/hdbretrieve.py'],
    url='',
    license='',
    author='Christophe Alexandre',
    author_email='ch.alexandre@bluewin.ch',
    description='Extracting building data from HDB website',
    long_description=read('README.md'),
    entry_points={
        'console_scripts': [
            'hdbretrieve = hdb:main',
        ],
    },
    install_requires = [
        'beautifulsoup4>=4.4.1',
        'retrying>=1.3.3',
        'pandas>=0.18.0',
        'requests>=2.10.0',
        'lxml>=3.6.0',
        'xlsxwriter>=0.9.2',
    ],
)
