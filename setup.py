"""
Setup script for TaskMasterPy.
"""
from setuptools import setup, find_packages

setup(
    name="taskmasterpy",
    version="0.1.0",
    description="A Python-based automation framework for data operations",
    author="TaskMasterPy Team",
    author_email="taskmasterpy@gmail.com",
    url="https://github.com/taskmasterpy/taskmasterpy",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.18.0",
        "pyyaml>=5.1",
        "typer>=0.3.0",
        "rich>=10.0.0",
        "schedule>=1.0.0",
        "requests>=2.25.0",
        "watchdog>=2.1.0",
        "jmespath>=0.10.0",
    ],
    extras_require={
        "excel": ["openpyxl>=3.0.0"],
        "webhook": ["flask>=2.0.0"],
        "win-notify": ["win10toast>=0.9"],
    },
    entry_points={
        "console_scripts": [
            "taskmaster=taskmaster.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    python_requires=">=3.8",
)
