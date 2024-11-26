from setuptools import find_packages, setup

setup(
    name="30_days_dagster",
    packages=find_packages(exclude=["30_days_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
