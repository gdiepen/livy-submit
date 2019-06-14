from setuptools import setup, find_packages
import os

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='Livy-Submit',
    url='https://github.com/gdiepen/livy-submit',
    author='Guido Diepen',
    author_email='site-github@guidodiepen.nl',
    # Needed to actually package something
    packages=find_packages(),
    # Needed for dependencies
    install_requires=[],
    # *strongly* suggested for sharing
    version='1.0',
    # The license can be anything you like
    license='MIT',
    description='Livy-Submit enables you to send your python files to a spark cluster using Livy running on edge node',
    long_description=open('README.md').read(),
    entry_points={'console_scripts' : [
            'livy_submit = livysubmit.__main__:main'
        ]}
)