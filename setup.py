from setuptools import setup, find_packages

setup(
    name='xpdan',
    version='0.0.0-alpha',
    packages=find_packages(),
    description='data processing module',
    zip_safe=False,
    package_data={'xpdan': ['data/']},
    url='http:/github.com/xpdAcq/xpdAn'
)
