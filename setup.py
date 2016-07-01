from distutils.core import setup

setup(
    name='singapore-hdb',
    version='0.1',
    packages=['hdb'],
    package_dir={'hdb': 'src/hdb'},
    scripts=['scripts/hdbretrieve.py'],
    url='',
    license='',
    author='Christophe Alexandre',
    author_email='ch.alexandre@bluewin.ch',
    description='Loading building data from HDB',
    install_requires=['bs4>=4-4.4.1', 'retrying>=1.3.3', 'pandas>=0.18.1', 'requests>=2.10.0', 'requests_cache>=0.4.12']
)
