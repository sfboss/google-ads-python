#!/usr/bin/env python3
"""
Google Ads Keyword Research CLI Package Setup
"""

from setuptools import setup, find_packages

setup(
    name="google-ads-keyword-cli",
    version="1.0.0",
    description="Professional Google Ads Keyword Research CLI",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "google-ads>=24.0.0",
    ],
    entry_points={
        "console_scripts": [
            "kwcli=kwcli:main",
        ],
    },
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
