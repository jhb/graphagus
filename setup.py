from setuptools import setup, find_packages
import sys, os

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

version = '0.1'

setup(name='graphagus',
      version=version,
      description="A graph database for property graphs on top of ZODB",
      long_description="""Please read https://rawgit.com/jhb/graphagus/master/graphagus/README.html""",
      classifiers=["License :: OSI Approved :: GNU General Public License (GPL)",
          "Programming Language :: Python",
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Developers",
          "Topic :: Database",], 
      keywords='graph ZODB',
      author='Joerg Baach',
      author_email='mail@baach.de',
      url='https://github.com/jhb/graphagus',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
          'ZODB',
          'ZEO',
          'repoze.catalog',
          'graphviz'
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
