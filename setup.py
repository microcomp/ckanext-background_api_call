from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
    name='ckanext-background_api_call',
    version=version,
    description="",
    long_description='''
    ''',
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Janos Farkas',
    author_email='farkas@microcomp.sk',
    url='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.background_api_call'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points='''
        [ckan.plugins]
        # Add plugins here, e.g.
        background_api_call=ckanext.background_api_call.plugin:BackgroundApiCall

        [ckan.celery_task]
        tasks = ckanext.background_api_call.celery_import:task_imports
    ''',
)
