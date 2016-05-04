from setuptools import setup
from setuptools import find_packages

setup(
    name='drow',
    version='0.1.0',
    description='A flexible, user-friendly Riak ORM',
    long_description='A flexible, user-friendly Riak ORM loosely based '
                     'off of Django that allows for easy querying, access '
                     'and data integrity, without requiring a given data '
                     'type.',
    url='https://github.com/Sendhub/drow',
    author='Max Smythe',
    author_email='max.smythe+drow@gmail.com',
    packages=find_packages(),
    install_requires=['riak'],
    license='MIT',
    keywords=['riak', 'orm']
)
