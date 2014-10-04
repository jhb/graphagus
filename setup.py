from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='graphagus',
      version=version,
      description="A graph database on top of ZODB",
      long_description="""\
""",
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
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
