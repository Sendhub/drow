from setuptools import setup
from setuptools import find_packages

setup(
    name='Drow',
    version='0.1.0',
    description='A flexible, user-friendly Riak ORM',
    long_description='A flexible, user-friendly Riak ORM loosely based '
                     'off of Django that allows for easy querying, access '
                     'and data integrity, without requiring a given data '
                     'type.',
    author='Max Smythe',
    packages=find_packages(),
    install_requires=['riak'],
    license='MIT',
    keywords=['riak', 'orm']
)