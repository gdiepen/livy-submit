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
    version='1.1.0',
    # The license can be anything you like
    license='MIT',
    description='Livy-Submit enables you to send your python files to a spark cluster using Livy running on edge node',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    entry_points={'console_scripts' : [
            'livy_submit = livysubmit.__main__:main'
        ]},
    classifiers         = [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
