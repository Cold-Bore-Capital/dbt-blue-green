from setuptools import setup, find_packages

setup(
    name='dbt_blue_green',
    version='1.0.0',
    description='A script for managing DBT-based blue-green deployments',
    author='Your Name',  # Replace with your name or organization
    author_email='your.email@example.com',  # Replace with your email
    packages=find_packages(where='src'),  # Adjust package location (src is inferred from your project)
    package_dir={'': 'src'},  # This maps where to find the packages
    install_requires=[
        'snowflake-connector-python'
    ],
    entry_points={
        'console_scripts': [
            'dbt-blue-green=cmd:main',  # Allows launching the script using `dbt-blue-green`
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',  # Replace with your project's license
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',  # Adjust based on your minimum Python requirement
)