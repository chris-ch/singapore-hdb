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
)
