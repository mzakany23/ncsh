from setuptools import setup, find_packages

setup(
    name="ncsoccer",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'scrapy>=2.0.0',
        'boto3>=1.0.0',
    ],
    python_requires='>=3.7',
)