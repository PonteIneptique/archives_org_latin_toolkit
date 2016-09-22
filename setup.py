from setuptools import setup, find_packages


setup(
    name='archives_org_latin_toolkit',
    version="0.0.2",
    description='Tools to parse and search across http://www.cs.cmu.edu/~dbamman/latin.html',
    url='http://github.com/ponteineptique/archives_org_latin',
    author='Thibault Clérice',
    author_email='leponteineptique@gmail.com',
    license='MIT',
    packages=find_packages(exclude=("test")),
    install_requires=[
        "pandas==0.18.1"
    ],
    test_suite="test",
    zip_safe=False
)
