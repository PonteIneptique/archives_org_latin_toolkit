from setuptools import setup, find_packages


setup(
    name='archives_org_latin_toolkit',
    version="0.0.1",
    description='Tools to parse and search across http://www.cs.cmu.edu/~dbamman/latin.html',
    url='http://github.com/ponteineptique/archives_org_latin',
    author='Thibault Clérice',
    author_email='leponteineptique@gmail.com',
    license='MIT',
    packages=find_packages(exclude=("tests")),
    install_requires=[
        "pandas>=0.17.1"
    ],
    test_suite="tests",
    zip_safe=False
)
