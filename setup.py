from setuptools import setup, find_packages

PACKAGE = "dbff"
NAME = "dbff"
VERSION = "1.4.4"

setup(name=NAME,
      version=VERSION,
      author="Xiayi Li",
      author_email="hi@xiayi.li",
      url="https://github.com/yosg/dbff",
      packages=find_packages(),
      description="Compare tables and rows between MySQL databases.",
      license="MIT License",
      install_requires=["MySQL-python"],
      py_modules=["dbff"]
)
