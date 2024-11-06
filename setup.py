from setuptools import setup, find_packages

setup(
    name="crosscheck_fingerprint_caller",
    version="0.1",
    description="To call swaps and matches from CrosscheckFingerprints output",
    author="Savo Lazic",
    author_email="savo.lazic@oicr.on.ca",
    python_requires=">=3.13.0",
    packages=find_packages(exclude=["test"]),
    entry_points={
        "console_scripts": [
            "crosscheck-fingerprint-caller = crosscheck_fingerprint_caller.main:main",
        ]
    },
    install_requires=[
        "pandas>=2.2",
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    test_suite="test",
    extras_require={
        "develop": ["pre-commit>=1.18.3", "pytest>=5.2.2", "pytest-runner>=5.2"]
    },
)
